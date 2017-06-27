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
	"strings"
	"time"

	"github.com/golang/glog"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	fedclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_clientset"
	"k8s.io/kubernetes/federation/pkg/dnsprovider"
	"k8s.io/kubernetes/federation/pkg/federation-controller/service/ingress"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util"
	"k8s.io/kubernetes/pkg/api/v1"
	corelisters "k8s.io/kubernetes/pkg/client/listers/core/v1"
)

const (
	ControllerName = "service-dns"

	UserAgentName = "federation-service-dns-controller"

	serviceSyncPeriod = 30 * time.Second
)

type ServiceDNSController struct {
	*util.AbstractDNSController
	// Client to federation api server
	federationClient fedclientset.Interface
	dns              dnsprovider.Interface
	federationName   string
	// serviceDNSSuffix is the DNS suffix we use when publishing service DNS names
	serviceDNSSuffix string
	// zoneName and zoneID are used to identify the zone in which to put records
	zoneName string
	zoneID   string
	dnsZones dnsprovider.Zones
	// each federation should be configured with a single zone (e.g. "mycompany.com")
	dnsZone dnsprovider.Zone
	// Informer Store for federated services
	serviceStore corelisters.ServiceLister
	// Informer controller for federated services
	serviceController cache.Controller
	workQueue         workqueue.Interface
}

// NewServiceDNSController returns a new service dns controller to manage DNS records for federated services
func NewServiceDNSController(client fedclientset.Interface, dnsProvider, dnsProviderConfig, federationName,
	serviceDNSSuffix, zoneName, zoneID string) (*ServiceDNSController, error) {
	dns, err := dnsprovider.InitDnsProvider(dnsProvider, dnsProviderConfig)
	if err != nil {
		runtime.HandleError(fmt.Errorf("DNS provider could not be initialized: %v", err))
		return nil, err
	}
	a := &util.AbstractDNSController{}
	d := &ServiceDNSController{
		AbstractDNSController:	a,
		federationClient: client,
		dns:              dns,
		federationName:   federationName,
		serviceDNSSuffix: serviceDNSSuffix,
		zoneName:         zoneName,
		zoneID:           zoneID,
		workQueue:        workqueue.New(),
	}
	a.DNSController = d;
	if err := d.validateConfig(); err != nil {
		runtime.HandleError(fmt.Errorf("Invalid configuration passed to DNS provider: %v", err))
		return nil, err
	}
	if err := d.retrieveOrCreateDNSZone(); err != nil {
		runtime.HandleError(fmt.Errorf("Failed to retrieve DNS zone: %v", err))
		return nil, err
	}

	// Start informer in federated API servers on federated services
	var serviceIndexer cache.Indexer
	serviceIndexer, d.serviceController = cache.NewIndexerInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (pkgruntime.Object, error) {
				return client.Core().Services(metav1.NamespaceAll).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return client.Core().Services(metav1.NamespaceAll).Watch(options)
			},
		},
		&v1.Service{},
		serviceSyncPeriod,
		util.NewTriggerOnAllChanges(func(obj pkgruntime.Object) { d.workQueue.Add(obj) }),
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
	d.serviceStore = corelisters.NewServiceLister(serviceIndexer)

	return d, nil
}

func (s *ServiceDNSController) DNSControllerRun(workers int, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()
	defer s.workQueue.ShutDown()

	glog.Infof("Starting federation service dns controller")
	defer glog.Infof("Stopping federation service dns controller")

	go s.serviceController.Run(stopCh)

	for i := 0; i < workers; i++ {
		go wait.Until(s.worker, time.Second, stopCh)
	}

	<-stopCh
}

func wantsDNSRecords(service *v1.Service) bool {
	return service.Spec.Type == v1.ServiceTypeLoadBalancer
}

func (s *ServiceDNSController) workerFunction() bool {
	item, quit := s.workQueue.Get()
	if quit {
		return true
	}
	defer s.workQueue.Done(item)

	service := item.(*v1.Service)

	if !wantsDNSRecords(service) {
		return false
	}

	ingress, err := ingress.ParseFederatedServiceIngress(service)
	if err != nil {
		runtime.HandleError(fmt.Errorf("Error in parsing lb ingress for service %s/%s: %v", service.Namespace, service.Name, err))
		return false
	}
	for _, clusterIngress := range ingress.Items {
		s.ensureDNSRecords(clusterIngress.Cluster, service)
	}
	return false
}

func (s *ServiceDNSController) worker() {
	for {
		if quit := s.workerFunction(); quit {
			glog.Infof("service dns controller worker queue shutting down")
			return
		}
	}
}

func (s *ServiceDNSController) validateConfig() error {
	if s.federationName == "" {
		return fmt.Errorf("DNSController should not be run without federationName")
	}
	if s.zoneName == "" && s.zoneID == "" {
		return fmt.Errorf("DNSController must be run with either zoneName or zoneID")
	}
	if s.serviceDNSSuffix == "" {
		if s.zoneName == "" {
			return fmt.Errorf("DNSController must be run with zoneName, if serviceDnsSuffix is not set")
		}
		s.serviceDNSSuffix = s.zoneName
	}
	if s.dns == nil {
		return fmt.Errorf("DNSController should not be run without a dnsprovider")
	}
	zones, ok := s.dns.Zones()
	if !ok {
		return fmt.Errorf("the dns provider does not support zone enumeration, which is required for creating dns records")
	}
	s.dnsZones = zones
	return nil
}

func (s *ServiceDNSController) retrieveOrCreateDNSZone() error {
	matchingZones, err := s.GetDNSZones(s.zoneName, s.zoneID, s.dnsZones)
	if err != nil {
		return fmt.Errorf("error querying for DNS zones: %v", err)
	}
	switch len(matchingZones) {
	case 0: // No matching zones for s.zoneName, so create one
		if s.zoneName == "" {
			return fmt.Errorf("DNSController must be run with zoneName to create zone automatically")
		}
		glog.Infof("DNS zone %q not found.  Creating DNS zone %q.", s.zoneName, s.zoneName)
		managedZone, err := s.dnsZones.New(s.zoneName)
		if err != nil {
			return err
		}
		zone, err := s.dnsZones.Add(managedZone)
		if err != nil {
			return err
		}
		glog.Infof("DNS zone %q successfully created.  Note that DNS resolution will not work until you have registered this name with "+
			"a DNS registrar and they have changed the authoritative name servers for your domain to point to your DNS provider", zone.Name())
	case 1: // s.zoneName matches exactly one DNS zone
		s.dnsZone = matchingZones[0]
	default: // s.zoneName matches more than one DNS zone
		return fmt.Errorf("Multiple matching DNS zones found for %q; please specify zoneID", s.zoneName)
	}
	return nil
}

// getHealthyEndpoints returns the hostnames and/or IP addresses of healthy endpoints for the service, at a zone, region and global level (or an error)
func (s *ServiceDNSController) getHealthyEndpoints(clusterName string, service *v1.Service) (zoneEndpoints, regionEndpoints, globalEndpoints []string, err error) {
	var (
		zoneNames  []string
		regionName string
	)
	if zoneNames, regionName, err = s.getClusterZoneNames(clusterName); err != nil {
		return nil, nil, nil, err
	}

	// If federated service is deleted, return empty endpoints, so that DNS records are removed
	if service.DeletionTimestamp != nil {
		return zoneEndpoints, regionEndpoints, globalEndpoints, nil
	}

	serviceIngress, err := ingress.ParseFederatedServiceIngress(service)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, lbClusterIngress := range serviceIngress.Items {
		lbClusterName := lbClusterIngress.Cluster
		lbZoneNames, lbRegionName, err := s.getClusterZoneNames(lbClusterName)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, ingress := range lbClusterIngress.Items {
			var address string
			// We should get either an IP address or a hostname - use whichever one we get
			if ingress.IP != "" {
				address = ingress.IP
			} else if ingress.Hostname != "" {
				address = ingress.Hostname
			}
			if len(address) <= 0 {
				return nil, nil, nil, fmt.Errorf("Service %s/%s in cluster %s has neither LoadBalancerStatus.ingress.ip nor LoadBalancerStatus.ingress.hostname. Cannot use it as endpoint for federated service",
					service.Name, service.Namespace, clusterName)
			}
			for _, lbZoneName := range lbZoneNames {
				for _, zoneName := range zoneNames {
					if lbZoneName == zoneName {
						zoneEndpoints = append(zoneEndpoints, address)
					}
				}
			}
			if lbRegionName == regionName {
				regionEndpoints = append(regionEndpoints, address)
			}
			globalEndpoints = append(globalEndpoints, address)
		}
	}
	return zoneEndpoints, regionEndpoints, globalEndpoints, nil
}

// getClusterZoneNames returns the name of the zones (and the region) where the specified cluster exists (e.g. zones "us-east1-c" on GCE, or "us-east-1b" on AWS)
func (s *ServiceDNSController) getClusterZoneNames(clusterName string) ([]string, string, error) {
	cluster, err := s.federationClient.Federation().Clusters().Get(clusterName, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}
	return cluster.Status.Zones, cluster.Status.Region, nil
}

/* ensureDNSRecords ensures (idempotently, and with minimum mutations) that all of the DNS records for a service in a given cluster are correct,
given the current state of that service in that cluster.  This should be called every time the state of a service might have changed
(either w.r.t. its loadbalancer address, or if the number of healthy backend endpoints for that service transitioned from zero to non-zero
(or vice versa).  Only shards of the service which have both a loadbalancer ingress IP address or hostname AND at least one healthy backend endpoint
are included in DNS records for that service (at all of zone, region and global levels). All other addresses are removed.  Also, if no shards exist
in the zone or region of the cluster, a CNAME reference to the next higher level is ensured to exist. */
func (s *ServiceDNSController) ensureDNSRecords(clusterName string, service *v1.Service) error {
	// Quinton: Pseudocode....
	// See https://github.com/kubernetes/kubernetes/pull/25107#issuecomment-218026648
	// For each service we need the following DNS names:
	// mysvc.myns.myfed.svc.z1.r1.mydomain.com  (for zone z1 in region r1)
	//         - an A record to IP address of specific shard in that zone (if that shard exists and has healthy endpoints)
	//         - OR a CNAME record to the next level up, i.e. mysvc.myns.myfed.svc.r1.mydomain.com  (if a healthy shard does not exist in zone z1)
	// mysvc.myns.myfed.svc.r1.mydomain.com
	//         - a set of A records to IP addresses of all healthy shards in region r1, if one or more of these exist
	//         - OR a CNAME record to the next level up, i.e. mysvc.myns.myfed.svc.mydomain.com (if no healthy shards exist in region r1)
	// mysvc.myns.myfed.svc.mydomain.com
	//         - a set of A records to IP addresses of all healthy shards in all regions, if one or more of these exist.
	//         - no record (NXRECORD response) if no healthy shards exist in any regions
	//
	// Each service has the current known state of loadbalancer ingress for the federated cluster stored in annotations.
	// So generate the DNS records based on the current state and ensure those desired DNS records match the
	// actual DNS records (add new records, remove deleted records, and update changed records).
	//
	serviceName := service.Name
	namespaceName := service.Namespace
	zoneNames, regionName, err := s.getClusterZoneNames(clusterName)
	if err != nil {
		return err
	}
	if zoneNames == nil {
		return fmt.Errorf("failed to get cluster zone names")
	}
	zoneEndpoints, regionEndpoints, globalEndpoints, err := s.getHealthyEndpoints(clusterName, service)
	if err != nil {
		return err
	}
	commonPrefix := serviceName + "." + namespaceName + "." + s.federationName + ".svc"
	// dnsNames is the path up the DNS search tree, starting at the leaf
	dnsNames := []string{
		strings.Join([]string{commonPrefix, zoneNames[0], regionName, s.serviceDNSSuffix}, "."), // zone level - TODO might need other zone names for multi-zone clusters
		strings.Join([]string{commonPrefix, regionName, s.serviceDNSSuffix}, "."),               // region level, one up from zone level
		strings.Join([]string{commonPrefix, s.serviceDNSSuffix}, "."),                           // global level, one up from region level
		"", // nowhere to go up from global level
	}

	endpoints := [][]string{zoneEndpoints, regionEndpoints, globalEndpoints}

	for i, endpoint := range endpoints {
		if err = s.EnsureDNSRrsets(s.dnsZone, dnsNames[i], endpoint, dnsNames[i+1]); err != nil {
			return err
		}
	}
	return nil
}

func FindRrset(list []dnsprovider.ResourceRecordSet, rrset dnsprovider.ResourceRecordSet) dnsprovider.ResourceRecordSet {
	for i, elem := range list {
		if dnsprovider.ResourceRecordSetsEquivalent(rrset, elem) {
			return list[i]
		}
	}
	return nil
}