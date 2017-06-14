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
	"testing"

	"k8s.io/kubernetes/federation/apis/federation/v1beta1"
	. "k8s.io/kubernetes/federation/pkg/federation-controller/util/test"
	"k8s.io/kubernetes/pkg/api/v1"
	"k8s.io/kubernetes/federation/pkg/dnsprovider"
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

/*	cluster1Name := "c1"
	cluster2Name := "c2"
	cluster1 := NewClusterWithRegionZone(cluster1Name, v1.ConditionTrue, "fooregion", "foozone")
	cluster2 := NewClusterWithRegionZone(cluster2Name, v1.ConditionTrue, "barregion", "barzone")
	globalDNSName := "servicename.servicenamespace.myfederation.svc.federation.example.com"
	fooRegionDNSName := "servicename.servicenamespace.myfederation.svc.fooregion.federation.example.com"
	fooZoneDNSName := "servicename.servicenamespace.myfederation.svc.foozone.fooregion.federation.example.com"
	barRegionDNSName := "servicename.servicenamespace.myfederation.svc.barregion.federation.example.com"
	barZoneDNSName := "servicename.servicenamespace.myfederation.svc.barzone.barregion.federation.example.com"
*/

}
