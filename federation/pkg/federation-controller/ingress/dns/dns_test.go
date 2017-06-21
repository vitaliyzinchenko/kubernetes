/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dns

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
	//"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/federation/apis/federation/v1beta1"
	fakefedclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_clientset/fake"
	"k8s.io/kubernetes/federation/pkg/dnsprovider/providers/google/clouddns" // Only for unit testing purposes.
	"k8s.io/kubernetes/federation/pkg/dnsprovider" // Only for unit testing purposes.
	. "k8s.io/kubernetes/federation/pkg/federation-controller/util/test"
	"k8s.io/kubernetes/pkg/api/v1"
	ic "k8s.io/kubernetes/federation/pkg/federation-controller/ingress"
	extensionsv1beta1 "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
)

// NewClusterWithRegionZone builds a new cluster object with given region and zone attributes.
func NewClusterWithRegionZone(name string, readyStatus v1.ConditionStatus, region, zone string) *v1beta1.Cluster {
	cluster := NewCluster(name, readyStatus)
	cluster.Status.Zones = []string{zone}
	cluster.Status.Region = region
	return cluster
}

func TestServiceController_ensureDnsRecords(t *testing.T) {
	_, _ = dnsprovider.InitDnsProvider("coredns", "")

	cluster1Name := "c1"
	cluster2Name := "c2"
	cluster1 := NewClusterWithRegionZone(cluster1Name, v1.ConditionTrue, "fooregion", "foozone")
	cluster2 := NewClusterWithRegionZone(cluster2Name, v1.ConditionTrue, "barregion", "barzone")
	globalDNSName := "ingname.ingns.myfederation.ing.federation.example.com"
	fooRegionDNSName := "ingname.ingns.myfederation.ing.fooregion.federation.example.com"
	fooZoneDNSName := "ingname.ingns.myfederation.ing.foozone.fooregion.federation.example.com"
	barRegionDNSName := "ingname.ingns.myfederation.ing.barregion.federation.example.com"
	barZoneDNSName := "ingname.ingns.myfederation.ing.barzone.barregion.federation.example.com"

	tests := []struct {
		name     string
		ingress  extensionsv1beta1.Ingress
		expected []string
	}{
		{
			name: "ServiceWithSingleLBIngress",
			ingress: extensionsv1beta1.Ingress{

				ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
					ic.GlobalIngressLBStatus: BuildAnnotation(map[string][]string{
						cluster1Name : {"198.51.100.1", "198.51.100.2"},
						cluster2Name : {},
					})},
				},
			},
			expected: []string{
				"example.com:" + globalDNSName + ":A:180:[198.51.100.1 198.51.100.2]",
				"example.com:" + fooRegionDNSName + ":A:180:[198.51.100.1 198.51.100.2]",
				"example.com:" + fooZoneDNSName + ":A:180:[198.51.100.1 198.51.100.2]",
				"example.com:" + barRegionDNSName + ":CNAME:180:[" + globalDNSName + "]",
				"example.com:" + barZoneDNSName + ":CNAME:180:[" + barRegionDNSName + "]",
			},
		},
	}

	for _, test := range tests {
		fakedns, _ := clouddns.NewFakeInterface()
		fakednsZones, ok := fakedns.Zones()
		if !ok {
			t.Error("Unable to fetch zones")
		}
		fakeClient := &fakefedclientset.Clientset{}
		RegisterFakeClusterGet(&fakeClient.Fake, &v1beta1.ClusterList{Items: []v1beta1.Cluster{*cluster1, *cluster2}})
		d := IngressDNSController{
			federationClient: fakeClient,
			dns:              fakedns,
			dnsZones:         fakednsZones,
			ingressDNSSuffix: "federation.example.com",
			zoneName:         "example.com",
			federationName:   "myfederation",
		}

		dnsZones, err := getDNSZones(d.zoneName, d.zoneID, d.dnsZones)
		if err != nil {
			t.Errorf("Test failed for %s, Get DNS Zones failed: %v", test.name, err)
		}
		d.dnsZone = dnsZones[0]
		test.ingress.Name = "ingname"
		test.ingress.Namespace = "ingns"

		d.ensureDNSRecords(cluster1Name, &test.ingress)
		d.ensureDNSRecords(cluster2Name, &test.ingress)

		zones, err := fakednsZones.List()
		if err != nil {
			t.Errorf("error querying zones: %v", err)
		}

		// Dump every record to a testable-by-string-comparison form
		records := []string{}
		for _, z := range zones {
			zoneName := z.Name()

			rrs, ok := z.ResourceRecordSets()
			if !ok {
				t.Errorf("cannot get rrs for zone %q", zoneName)
			}

			rrList, err := rrs.List()
			if err != nil {
				t.Errorf("error querying rr for zone %q: %v", zoneName, err)
			}
			for _, rr := range rrList {
				rrdatas := rr.Rrdatas()

				// Put in consistent (testable-by-string-comparison) order
				sort.Strings(rrdatas)
				records = append(records, fmt.Sprintf("%s:%s:%s:%d:%s", zoneName, rr.Name(), rr.Type(), rr.Ttl(), rrdatas))
			}
		}

		// Ignore order of records
		sort.Strings(records)
		sort.Strings(test.expected)
		t.Logf("%v \n", records)

		if !reflect.DeepEqual(records, test.expected) {
			t.Errorf("Test %q failed.  Actual=%v, Expected=%v", test.name, records, test.expected)
		}

	}
}

func BuildAnnotation(clusters map[string][]string) string {
	globalLbStat := make(map[string][]v1.LoadBalancerStatus)
	for clusterName, hosts := range clusters {
		lbStatus := v1.LoadBalancerStatus{Ingress: make([]v1.LoadBalancerIngress, len(hosts))}
		for i := 0; i <  len(hosts); i++{
			lbStatus.Ingress[i] = v1.LoadBalancerIngress{IP: hosts[i]}
		}
		globalLbStat[clusterName] = append(globalLbStat[clusterName], lbStatus)
	}

	return ic.BuildGlobalLbStatusAnnotation(globalLbStat)

}
