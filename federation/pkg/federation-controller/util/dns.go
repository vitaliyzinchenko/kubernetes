package util

import (
	"net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/federation/pkg/dnsprovider"
	"github.com/golang/glog"
	"k8s.io/kubernetes/federation/pkg/dnsprovider/rrstype"
	"fmt"
)

const (
	// minDNSTTL is the minimum safe DNS TTL value to use (in seconds).  We use this as the TTL for all DNS records.
	minDNSTTL = 180
)

type DNSController interface {
	GetResolvedEndpoints(endpoints []string) ([]string, error)
	GetDNSZones(dnsZoneName string, dnsZoneID string, dnsZonesInterface dnsprovider.Zones) ([]dnsprovider.Zone, error)
	GetRrset(dnsName string, rrsetsInterface dnsprovider.ResourceRecordSets) ([]dnsprovider.ResourceRecordSet, error)
	FindRrset(list []dnsprovider.ResourceRecordSet, rrset dnsprovider.ResourceRecordSet) dnsprovider.ResourceRecordSet
	EnsureDNSRrsets(dnsZone dnsprovider.Zone, dnsName string, endpoints []string, uplevelCname string) error
}

type AbstractDNSController struct {
	DNSController
}

/* getResolvedEndpoints performs DNS resolution on the provided slice of endpoints (which might be DNS names or IPv4 addresses)
   and returns a list of IPv4 addresses.  If any of the endpoints are neither valid IPv4 addresses nor resolvable DNS names,
   non-nil error is also returned (possibly along with a partially complete list of resolved endpoints.
*/
func (a *AbstractDNSController) GetResolvedEndpoints(endpoints []string) ([]string, error) {
	resolvedEndpoints := sets.String{}
	for _, endpoint := range endpoints {
		if net.ParseIP(endpoint) == nil {
			// It's not a valid IP address, so assume it's a DNS name, and try to resolve it,
			// replacing its DNS name with its IP addresses in expandedEndpoints
			ipAddrs, err := net.LookupHost(endpoint)
			if err != nil {
				return resolvedEndpoints.List(), err
			}
			for _, ip := range ipAddrs {
				resolvedEndpoints = resolvedEndpoints.Union(sets.NewString(ip))
			}
		} else {
			resolvedEndpoints = resolvedEndpoints.Union(sets.NewString(endpoint))
		}
	}
	return resolvedEndpoints.List(), nil
}

// NOTE: that if the named resource record set does not exist, but no
// error occurred, the returned list will be empty, and the error will
// be nil
func GetRrset(dnsName string, rrsetsInterface dnsprovider.ResourceRecordSets) ([]dnsprovider.ResourceRecordSet, error) {
	return rrsetsInterface.Get(dnsName)
}

/* ensureDNSRrsets ensures (idempotently, and with minimum mutations) that all of the DNS resource record sets for dnsName are consistent with endpoints.
   if endpoints is nil or empty, a CNAME record to uplevelCname is ensured.
*/
func (s *AbstractDNSController) EnsureDNSRrsets(dnsZone dnsprovider.Zone, dnsName string, endpoints []string, uplevelCname string) error {
	rrsets, supported := dnsZone.ResourceRecordSets()
	if !supported {
		return fmt.Errorf("Failed to ensure DNS records for %s. DNS provider does not support the ResourceRecordSets interface", dnsName)
	}
	rrsetList, err := s.GetRrset(dnsName, rrsets) // TODO: rrsets.Get(dnsName)
	if err != nil {
		return err
	}
	if len(rrsetList) == 0 {
		glog.V(4).Infof("No recordsets found for DNS name %q.  Need to add either A records (if we have healthy endpoints), or a CNAME record to %q", dnsName, uplevelCname)
		if len(endpoints) < 1 {
			glog.V(4).Infof("There are no healthy endpoint addresses at level %q, so CNAME to %q, if provided", dnsName, uplevelCname)
			if uplevelCname != "" {
				glog.V(4).Infof("Creating CNAME to %q for %q", uplevelCname, dnsName)
				newRrset := rrsets.New(dnsName, []string{uplevelCname}, minDNSTTL, rrstype.CNAME)
				glog.V(4).Infof("Adding recordset %v", newRrset)
				err = rrsets.StartChangeset().Add(newRrset).Apply()
				if err != nil {
					return err
				}
				glog.V(4).Infof("Successfully created CNAME to %q for %q", uplevelCname, dnsName)
			} else {
				glog.V(4).Infof("We want no record for %q, and we have no record, so we're all good.", dnsName)
			}
		} else {
			// We have valid endpoint addresses, so just add them as A records.
			// But first resolve DNS names, as some cloud providers (like AWS) expose
			// load balancers behind DNS names, not IP addresses.
			glog.V(4).Infof("We have valid endpoint addresses %v at level %q, so add them as A records, after resolving DNS names", endpoints, dnsName)
			resolvedEndpoints, err := s.GetResolvedEndpoints(endpoints)
			if err != nil {
				return err // TODO: We could potentially add the ones we did get back, even if some of them failed to resolve.
			}
			newRrset := rrsets.New(dnsName, resolvedEndpoints, minDNSTTL, rrstype.A)
			glog.V(4).Infof("Adding recordset %v", newRrset)
			err = rrsets.StartChangeset().Add(newRrset).Apply()
			if err != nil {
				return err
			}
			glog.V(4).Infof("Successfully added recordset %v", newRrset)
		}
	} else {
		// the rrsets already exists, so make it right.
		glog.V(4).Infof("Recordset %v already exists. Ensuring that it is correct.", rrsetList)
		if len(endpoints) < 1 {
			// Need an appropriate CNAME record.  Check that we have it.
			newRrset := rrsets.New(dnsName, []string{uplevelCname}, minDNSTTL, rrstype.CNAME)
			glog.V(4).Infof("No healthy endpoints for %s. Have recordsets %v. Need recordset %v", dnsName, rrsetList, newRrset)
			found := s.FindRrset(rrsetList, newRrset)
			if found != nil {
				// The existing rrset is equivalent to the required one - our work is done here
				glog.V(4).Infof("Existing recordset %v is equivalent to needed recordset %v, our work is done here.", rrsetList, newRrset)
				return nil
			} else {
				// Need to replace the existing one with a better one (or just remove it if we have no healthy endpoints).
				glog.V(4).Infof("Existing recordset %v not equivalent to needed recordset %v removing existing and adding needed.", rrsetList, newRrset)
				changeSet := rrsets.StartChangeset()
				for i := range rrsetList {
					changeSet = changeSet.Remove(rrsetList[i])
				}
				if uplevelCname != "" {
					changeSet = changeSet.Add(newRrset)
					if err := changeSet.Apply(); err != nil {
						return err
					}
					glog.V(4).Infof("Successfully replaced needed recordset %v -> %v", found, newRrset)
				} else {
					if err := changeSet.Apply(); err != nil {
						return err
					}
					glog.V(4).Infof("Successfully removed existing recordset %v", found)
					glog.V(4).Infof("Uplevel CNAME is empty string. Not adding recordset %v", newRrset)
				}
			}
		} else {
			// We have an rrset in DNS, possibly with some missing addresses and some unwanted addresses.
			// And we have healthy endpoints.  Just replace what's there with the healthy endpoints, if it's not already correct.
			glog.V(4).Infof("%s: Healthy endpoints %v exist. Recordset %v exists.  Reconciling.", dnsName, endpoints, rrsetList)
			resolvedEndpoints, err := s.GetResolvedEndpoints(endpoints)
			if err != nil { // Some invalid addresses or otherwise unresolvable DNS names.
				return err // TODO: We could potentially add the ones we did get back, even if some of them failed to resolve.
			}
			newRrset := rrsets.New(dnsName, resolvedEndpoints, minDNSTTL, rrstype.A)
			glog.V(4).Infof("Have recordset %v. Need recordset %v", rrsetList, newRrset)
			found := s.FindRrset(rrsetList, newRrset)
			if found != nil {
				glog.V(4).Infof("Existing recordset %v is equivalent to needed recordset %v, our work is done here.", found, newRrset)
				// TODO: We could be more thorough about checking for equivalence to avoid unnecessary updates, but in the
				//       worst case we'll just replace what's there with an equivalent, if not exactly identical record set.
				return nil
			} else {
				// Need to replace the existing one with a better one
				glog.V(4).Infof("Existing recordset %v is not equivalent to needed recordset %v, removing existing and adding needed.", found, newRrset)
				changeSet := rrsets.StartChangeset()
				for i := range rrsetList {
					changeSet = changeSet.Remove(rrsetList[i])
				}
				changeSet = changeSet.Add(newRrset)
				if err = changeSet.Apply(); err != nil {
					return err
				}
				glog.V(4).Infof("Successfully replaced recordset %v -> %v", found, newRrset)
			}
		}
	}
	return nil
}