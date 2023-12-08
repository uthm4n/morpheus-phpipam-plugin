/*
* Copyright 2022 the original author or authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.morpheusdata.phpipam

import com.morpheusdata.core.DNSProvider
import com.morpheusdata.core.IPAMProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.util.ConnectionUtils
import com.morpheusdata.core.util.HttpApiClient
import com.morpheusdata.core.util.NetworkUtility
import com.morpheusdata.core.util.SyncTask
import com.morpheusdata.model.AccountIntegration
import com.morpheusdata.model.Icon
import com.morpheusdata.model.Network
import com.morpheusdata.model.NetworkDomain
import com.morpheusdata.model.NetworkDomainRecord
import com.morpheusdata.model.NetworkPool
import com.morpheusdata.model.NetworkPoolIp
import com.morpheusdata.model.NetworkPoolRange
import com.morpheusdata.model.NetworkPoolServer
import com.morpheusdata.model.NetworkPoolType
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.projection.NetworkDomainIdentityProjection
import com.morpheusdata.model.projection.NetworkDomainRecordIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIdentityProjection
import com.morpheusdata.model.projection.NetworkPoolIpIdentityProjection
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j
import io.reactivex.Single
import org.apache.commons.net.util.SubnetUtils
import org.apache.http.entity.ContentType
import io.reactivex.Observable
import org.apache.commons.validator.routines.InetAddressValidator

/**
 * The IPAM / DNS Provider implementation for {php}IPAM
 * This contains most methods used for interacting directly with the phpIPAM API
 * 
 * @author David Estes
 */
@Slf4j
class PhpIpamProvider implements IPAMProvider {

    MorpheusContext morpheusContext
    Plugin plugin

    PhpIpamProvider(Plugin plugin, MorpheusContext morpheusContext) {
        this.morpheusContext = morpheusContext
        this.plugin = plugin
    }



    /**
     * Validation Method used to validate all inputs applied to the integration of an IPAM Provider upon save.
     * If an input fails validation or authentication information cannot be verified, Error messages should be returned
     * via a {@link ServiceResponse} object where the key on the error is the field name and the value is the error message.
     * If the error is a generic authentication error or unknown error, a standard message can also be sent back in the response.
     *
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the inputs are valid or not.
     */
    @Override
    ServiceResponse verifyNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        ServiceResponse<NetworkPoolServer> rtn = ServiceResponse.error()
        log.info("verifyPoolServer: ${poolServer}")

        rtn.errors = [:]
        if(!poolServer.name || poolServer.name == ''){
            rtn.errors['name'] = 'name is required'
        }
        if(!poolServer.serviceUrl || poolServer.serviceUrl == ''){
            rtn.errors['serviceUrl'] = 'PHP IPAM API URL is required'
        }

        if((!poolServer.serviceUsername || poolServer.serviceUsername == '') && (!poolServer.credentialData?.username || poolServer.credentialData?.username == '')){
            rtn.errors['serviceUsername'] = 'username is required'
        }
        if((!poolServer.servicePassword || poolServer.servicePassword == '') && (!poolServer.credentialData?.password || poolServer.credentialData?.password == '')){
            rtn.errors['servicePassword'] = 'password is required'
        }

        if (!poolServer.configMap.appId) {
            rtn.errors.appId = "App ID is required" // todo: i18n
        }

        rtn.data = poolServer
        if(rtn.errors.size() > 0) {
            rtn.success = false
            return rtn //
        }
        
        HttpApiClient phpIpamClient = new HttpApiClient()
        try {
            def apiUrl = poolServer.serviceUrl
            boolean hostOnline = false
            try {
                def apiUrlObj = new URL(apiUrl)
                def apiHost = apiUrlObj.host
                def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
                hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, false, true, null)
            } catch(e) {
                log.error("Error parsing URL {}", apiUrl, e)
            }
            if(hostOnline) {
                opts.doPaging = false
                opts.maxResults = 1
                def tokenResults = getToken(phpIpamClient,poolServer)
                if(tokenResults.success) {
                    def networkList = listNetworks(phpIpamClient,tokenResults.data?.token as String,poolServer)
                    if(networkList.success) {
                        rtn.success = true
                    } else {
                        rtn.msg = networkList.msg ?: 'Error connecting to {php}IPAM'
                    }
                } else {
                    rtn.msg = "Auhentication failed ${tokenResults.msg}"
                }

            } else {
                rtn.msg = 'Host not reachable'
            }
        } catch(e) {
            log.error("verifyPoolServer error: ${e}", e)
        } finally {
            phpIpamClient.shutdownClient()
        }
        return rtn
    }

    /**
     * Called during creation of a {@link NetworkPoolServer} operation. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse createNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        return null
    }

    /**
     * Called during update of an existing {@link NetworkPoolServer}. This allows for any custom operations that need
     * to be performed outside of the standard operations.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts any custom payload submission options may exist here
     * @return A response is returned depending on if the operation was a success or not.
     */
    @Override
    ServiceResponse updateNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        return null
    }


    /**
     * Periodically called to refresh and sync data coming from the relevant integration. Most integration providers
     * provide a method like this that is called periodically (typically 5 - 10 minutes). DNS Sync operates on a 10min
     * cycle by default. Useful for caching Host Records created outside of Morpheus.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     */
    @Override
    void refresh(NetworkPoolServer poolServer) {
        log.debug("refreshNetworkPoolServer: {}", poolServer.dump())
        HttpApiClient phpIpamClient = new HttpApiClient()
        phpIpamClient.throttleRate = poolServer.serviceThrottleRate
        try {
            def apiUrl = poolServer.serviceUrl
            def apiUrlObj = new URL(apiUrl)
            def apiHost = apiUrlObj.host
            def apiPort = apiUrlObj.port > 0 ? apiUrlObj.port : (apiUrlObj?.protocol?.toLowerCase() == 'https' ? 443 : 80)
            def hostOnline = ConnectionUtils.testHostConnectivity(apiHost, apiPort, false, true, null)
            log.debug("online: {} - {}", apiHost, hostOnline)
            def testResults
            String token = null

            def loginResults
            // Promise
            if(hostOnline) {
                loginResults = getToken(phpIpamClient,poolServer)

                if(!loginResults.success) {
                    //NOTE invalidLogin was only ever set to false.
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, 'error calling {php}IPAM').blockingGet()
                } else {
                    token = loginResults.data.token as String
                    morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.syncing).blockingGet()
                }
            } else {
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.error, '{php}IPAM api not reachable')
            }
            Date now = new Date()
            if(loginResults?.success) {
                cacheNetworks(phpIpamClient,token,poolServer)
                if(poolServer?.configMap?.inventoryExisting) {
                    cacheIpAddressRecords(phpIpamClient,token,poolServer)
                }
                log.info("Sync Completed in ${new Date().time - now.time}ms")
                morpheus.network.updateNetworkPoolServerStatus(poolServer, AccountIntegration.Status.ok).subscribe().dispose()
            }
        } catch(e) {
            log.error("refreshNetworkPoolServer error: ${e}", e)
        } finally {
            logout(phpIpamClient,poolServer)
            phpIpamClient.shutdownClient()
        }
    }

    // cacheNetworks methods
    void cacheNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
        opts.doPaging = true
        def listResults = listNetworks(client, token, poolServer)
        if(listResults.success) {
            List apiItems = listResults.networks as List<Map>
            Observable<NetworkPoolIdentityProjection> poolRecords = morpheus.network.pool.listIdentityProjections(poolServer.id)
            SyncTask<NetworkPoolIdentityProjection,Map,NetworkPool> syncTask = new SyncTask(poolRecords, apiItems as Collection<Map>)
            syncTask.addMatchFunction { NetworkPoolIdentityProjection domainObject, Map apiItem ->
                domainObject.externalId == apiItem?.id?.toString()
            }.onDelete {removeItems ->
                morpheus.network.pool.remove(poolServer.id, removeItems).blockingGet()
            }.onAdd { itemsToAdd ->
                addMissingPools(poolServer, itemsToAdd)
            }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIdentityProjection,Map>> updateItems ->\
                Map<Long, SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                return morpheus.network.pool.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPool pool ->
                    SyncTask.UpdateItemDto<NetworkPoolIdentityProjection, Map> matchItem = updateItemMap[pool.id]
                    return new SyncTask.UpdateItem<NetworkPool,Map>(existingItem:pool, masterItem:matchItem.masterItem)
                }
            }.onUpdate { List<SyncTask.UpdateItem<NetworkPool,Map>> updateItems ->
                updateMatchedPools(poolServer, updateItems)
            }.start()
        }
    }

    void addMissingPools(NetworkPoolServer poolServer, Collection<Map> chunkedAddList) {
        def poolType = new NetworkPoolType(code: 'phpipam')
        def poolTypeIpv6 = new NetworkPoolType(code: 'phpipamipv6')
        List<NetworkPool> missingPoolsList = []
        chunkedAddList?.each { Map it ->
            def networkIp = "${it.subnet}/${it.mask}"
            def networkView = it.description
            networkView = networkView?.take(255 - (networkIp.size() + 1))
            def displayName = networkView ? (networkView + ' ' + networkIp) : networkIp
            def rangeConfig
            def addRange
            def newObj
            def addConfig
            if (!it.subnet.contains(':')) {
                def networkInfo = getNetworkPoolConfig(networkIp)
                addConfig = [poolServer: poolServer,account:poolServer.account, owner:poolServer.account, name:networkIp, externalId:it.id.toString(),
                                displayName:displayName, type:poolType, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id]
                addConfig += networkInfo.config
                newObj = new NetworkPool(addConfig)
                newObj.ipRanges = []
                networkInfo.ranges?.each { range ->
                    rangeConfig = [networkPool:newObj, startAddress:range.startAddress, endAddress:range.endAddress, addressCount:addConfig.ipCount]
                    addRange = new NetworkPoolRange(rangeConfig)
                    newObj.ipRanges.add(addRange)
                }
            } else {
                addConfig = [poolServer: poolServer,account:poolServer.account, owner:poolServer.account, name:networkIp, externalId:it.id.toString(),
                                displayName:displayName, type:poolTypeIpv6, poolEnabled:true, parentType:'NetworkPoolServer', parentId:poolServer.id]
                newObj = new NetworkPool(addConfig)
                newObj.ipRanges = []
                rangeConfig = [cidrIPv6: networkIp, startIPv6Address: it.subnet, endIPv6Address: it.subnet,addressCount:1,cidr: networkIp]
                addRange = new NetworkPoolRange(rangeConfig)
                newObj.ipRanges.add(addRange)
            }

            missingPoolsList.add(newObj)
        }
        morpheus.network.pool.create(poolServer.id, missingPoolsList).blockingGet()
    }

    void updateMatchedPools(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkPool,Map>> chunkedUpdateList) {
        List<NetworkPool> poolsToUpdate = []
        chunkedUpdateList?.each { update ->
            NetworkPool existingItem = update.existingItem
            if(existingItem) {
                //update view ?
                Boolean save = false
                def networkIp = "${update.masterItem.subnet}/${update.masterItem.mask}"
                def networkView = update.masterItem.description
                networkView = networkView?.take(255 - (networkIp.size() + 1))
                def displayName = networkView ? (networkView + ' ' + networkIp) : networkIp
                if(existingItem?.displayName != displayName) {
                    existingItem.displayName = displayName
                    save = true
                }
                if(save) {
                    poolsToUpdate << existingItem
                }
            }
        }
        if(poolsToUpdate.size() > 0) {
            morpheus.network.pool.save(poolsToUpdate).blockingGet()
        }
    }


    // cacheIpAddressRecords
    void cacheIpAddressRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts=[:]) {
        morpheus.network.pool.listIdentityProjections(poolServer.id).buffer(50).concatMap { Collection<NetworkPoolIdentityProjection> poolIdents ->
            return morpheus.network.pool.listById(poolIdents.collect{it.id})
        }.concatMap { NetworkPool pool ->
            def listResults = listHostRecords(client,token,poolServer,pool.externalId)
            if (listResults.success) {
                List<Map> apiItems = listResults.addresses
                Observable<NetworkPoolIpIdentityProjection> poolIps = morpheus.network.pool.poolIp.listIdentityProjections(pool.id)
                /**
                 * The match function for external id should not be used.
                 * In phpIpam the IP address can be updated for an existing ip address record. This update will keep the external ID the same but change the record to a different IP address.
                 * If this occurs within the PHP environment, there is a high chance the old IP will be used but with a different external ID and sync in twice into Morpheus with a duplicate IP.
                 * Instead of explicitly managing conflicting duplicate IPs when doing add/update (see duplicate per network pool constraint on NetworkPoolIp ipAddress field), instead sync on the IP address and update the external ID accordingly if it changes.
                 */
                SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp> syncTask = new SyncTask<NetworkPoolIpIdentityProjection, Map, NetworkPoolIp>(poolIps, apiItems)
                return syncTask.addMatchFunction { NetworkPoolIpIdentityProjection domainObject, Map apiItem ->
                    domainObject.ipAddress == apiItem?.ip
                }.onDelete {removeItems ->
                    morpheus.network.pool.poolIp.remove(pool.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingIps(pool, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection,Map>> updateItems ->

                    Map<Long, SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.pool.poolIp.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkPoolIp poolIp ->
                        SyncTask.UpdateItemDto<NetworkPoolIpIdentityProjection, Map> matchItem = updateItemMap[poolIp.id]
                        return new SyncTask.UpdateItem<NetworkPoolIp,Map>(existingItem:poolIp, masterItem:matchItem.masterItem)
                    }

                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedIps(updateItems)
                }.observe()
            } else {
                return Observable.fromArray([])
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.blockingSubscribe()
    }

    void addMissingIps(NetworkPool pool, List addList) {

        List<NetworkPoolIp> poolIpsToAdd = addList?.collect { it ->
            def ipAddress = it.ip
            def hostname = it.hostname ?: null
            def ipType = 'assigned'
            def addConfig = [networkPool: pool, networkPoolRange: pool.ipRanges ? pool.ipRanges.first() : null, ipType: ipType, hostname: hostname, ipAddress: ipAddress, externalId:it.id.toString()]
            def newObj = new NetworkPoolIp(addConfig)
            return newObj
        }
        if(poolIpsToAdd.size() > 0) {
            morpheus.network.pool.poolIp.create(pool, poolIpsToAdd).blockingGet()
        }
    }

    void updateMatchedIps(List<SyncTask.UpdateItem<NetworkPoolIp,Map>> updateList) {
        List<NetworkPoolIp> ipsToUpdate = []
        updateList?.each {  update ->
            NetworkPoolIp existingItem = update.existingItem

            if(existingItem) {
                def hostname = update.masterItem.hostname
                def ipType = 'assigned'
                def externalId = update.masterItem.id?.toString()

                Boolean save = false
                if(existingItem.ipType != ipType) {
                    existingItem.ipType = ipType
                    save = true
                }
                if(existingItem.hostname != hostname) {
                    existingItem.hostname = hostname
                    save = true
                }
                if(existingItem.externalId != externalId) {
                    existingItem.externalId = externalId
                    save = true
                }
                if(save) {
                    ipsToUpdate << existingItem
                }
            }
        }
        if(ipsToUpdate.size() > 0) {
            morpheus.network.pool.poolIp.save(ipsToUpdate).blockingGet()
        }
    }


    /**
     * TODO: Eventually do zone caching for this plugin
     * @param client
     * @param token
     * @param poolServer
     * @param opts
     * @return
     */
    def cacheZones(HttpApiClient client, String token, NetworkPoolServer poolServer, Map opts = [:]) {
        try {
            def listResults = listZones(client, token, poolServer)

            if (listResults.success) {
                List apiItems = listResults.zones as List<Map>
                Observable<NetworkDomainIdentityProjection> domainRecords = morpheus.network.domain.listIdentityProjections(poolServer.integration.id)

                SyncTask<NetworkDomainIdentityProjection,Map,NetworkDomain> syncTask = new SyncTask(domainRecords, apiItems as Collection<Map>)
                syncTask.addMatchFunction { NetworkDomainIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == apiItem.id?.toString()
                }.onDelete {removeItems ->
                    morpheus.network.domain.remove(poolServer.integration.id, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingZones(poolServer, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainIdentityProjection,Map>> updateItems ->
                    Map<Long, SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.domain.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomain networkDomain ->
                        SyncTask.UpdateItemDto<NetworkDomainIdentityProjection, Map> matchItem = updateItemMap[networkDomain.id]
                        return new SyncTask.UpdateItem<NetworkDomain,Map>(existingItem:networkDomain, masterItem:matchItem.masterItem)
                    }
                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomain,Map>> updateItems ->
                    updateMatchedZones(poolServer, updateItems)
                }.start()
            }
        } catch (e) {
            log.error("cacheZones error: ${e}", e)
        }
    }

    /**
     * Creates a mapping for networkDomainService.createSyncedNetworkDomain() method on the network context.
     * @param poolServer
     * @param addList
     */
    void addMissingZones(NetworkPoolServer poolServer, Collection addList) {
        List<NetworkDomain> missingZonesList = addList?.collect { Map add ->
            NetworkDomain networkDomain = new NetworkDomain([owner:poolServer.integration.account, refType:'AccountIntegration', refId:poolServer.integration.id,
                                                             externalId:add.id.toString(), name:NetworkUtility.getFriendlyDomainName(add.name), fqdn:NetworkUtility.getFqdnDomainName(add.name),
                                                             refSource:'integration', zoneType:'Authoritative'])

            return networkDomain
        }
        morpheus.network.domain.create(poolServer.integration.id, missingZonesList).blockingGet()
    }

    /**
     * Given a pool server and updateList, extract externalId's and names to match on and update NetworkDomains.
     * @param poolServer
     * @param addList
     */
    void updateMatchedZones(NetworkPoolServer poolServer, List<SyncTask.UpdateItem<NetworkDomain,Map>> updateList) {
        def domainsToUpdate = []
        for(SyncTask.UpdateItem<NetworkDomain,Map> update in updateList) {
            NetworkDomain existingItem = update.existingItem as NetworkDomain
            if(existingItem) {
                Boolean save = false
                if(!existingItem.externalId) {
                    existingItem.externalId = update.masterItem?.id?.toString()
                    save = true
                }
                if(!existingItem.refId) {
                    existingItem.refType = 'AccountIntegration'
                    existingItem.refId = poolServer.integration.id
                    existingItem.refSource = 'integration'
                    save = true
                }

                if(save) {
                    domainsToUpdate.add(existingItem)
                }
            }
        }
        if(domainsToUpdate.size() > 0) {
            morpheus.network.domain.save(domainsToUpdate).blockingGet()
        }
    }


    // Cache Zones methods
    def cacheZoneRecords(HttpApiClient client, NetworkPoolServer poolServer, Map opts=[:]) {
        morpheus.network.domain.listIdentityProjections(poolServer.integration.id).buffer(50).concatMap { Collection<NetworkDomainIdentityProjection> poolIdents ->
            return morpheus.network.domain.listById(poolIdents.collect{it.id})
        }.concatMap { NetworkDomain domain ->

            def listResults = listDnsResourceRecords(client,poolServer,opts + [queryParams:[WHERE:"dnszone_id=${domain.externalId}".toString()]])


            if (listResults.success) {
                List<Map> apiItems = listResults.data as List<Map>
                Observable<NetworkDomainRecordIdentityProjection> domainRecords = morpheus.network.domain.record.listIdentityProjections(domain,null)
                SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord> syncTask = new SyncTask<NetworkDomainRecordIdentityProjection, Map, NetworkDomainRecord>(domainRecords, apiItems)
                return syncTask.addMatchFunction {  NetworkDomainRecordIdentityProjection domainObject, Map apiItem ->
                    domainObject.externalId == apiItem.rr_id
                }.onDelete {removeItems ->
                    morpheus.network.domain.record.remove(domain, removeItems).blockingGet()
                }.onAdd { itemsToAdd ->
                    addMissingDomainRecords(domain, itemsToAdd)
                }.withLoadObjectDetails { List<SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection,Map>> updateItems ->

                    Map<Long, SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map>> updateItemMap = updateItems.collectEntries { [(it.existingItem.id): it]}
                    return morpheus.network.domain.record.listById(updateItems.collect{it.existingItem.id} as Collection<Long>).map { NetworkDomainRecord domainRecord ->
                        SyncTask.UpdateItemDto<NetworkDomainRecordIdentityProjection, Map> matchItem = updateItemMap[domainRecord.id]
                        return new SyncTask.UpdateItem<NetworkDomainRecord,Map>(existingItem:domainRecord, masterItem:matchItem.masterItem)
                    }

                }.onUpdate { List<SyncTask.UpdateItem<NetworkDomainRecord,Map>> updateItems ->
                    updateMatchedDomainRecords(updateItems)
                }.observe()
            } else {
                return Single.just(false)
            }
        }.doOnError{ e ->
            log.error("cacheIpRecords error: ${e}", e)
        }.blockingSubscribe()

    }


    void updateMatchedDomainRecords(List<SyncTask.UpdateItem<NetworkDomainRecord, Map>> updateList) {
        def records = []
        updateList?.each { update ->
            NetworkDomainRecord existingItem = update.existingItem
            String recordType = update.masterItem.rr_type
            if(existingItem) {
                //update view ?
                def save = false
                if(update.masterItem.rr_all_value != existingItem.content) {
                    existingItem.setContent(update.masterItem.rr_all_value as String)
                    save = true
                }

                if(update.masterItem.rr_full_name_utf != existingItem.name) {
                    existingItem.name = update.masterItem.rr_full_name_utf
                    existingItem.fqdn = update.masterItem.rr_full_name_utf
                    save = true
                }

                if(save) {
                    records.add(existingItem)
                }
            }
        }
        if(records.size() > 0) {
            morpheus.network.domain.record.save(records).blockingGet()
        }
    }

    void addMissingDomainRecords(NetworkDomainIdentityProjection domain, Collection<Map> addList) {
        List<NetworkDomainRecord> records = []

        addList?.each {
            String recordType = it.rr_type
            def addConfig = [networkDomain: new NetworkDomain(id: domain.id), externalId:it.rr_id, name: it.rr_full_name_utf, fqdn: it.rr_full_name_utf, type: recordType, source: 'sync']
            def newObj = new NetworkDomainRecord(addConfig)
            newObj.ttl = it.ttl?.toInteger()
            newObj.setContent(it.rr_all_value)
            records.add(newObj)
        }
        morpheus.network.domain.record.create(domain,records).blockingGet()
    }


    /**
     * Called on the first save / update of a pool server integration. Used to do any initialization of a new integration
     * Often times this calls the periodic refresh method directly.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param opts an optional map of parameters that could be sent. This may not currently be used and can be assumed blank
     * @return a ServiceResponse containing the success state of the initialization phase
     */
    @Override
    ServiceResponse initializeNetworkPoolServer(NetworkPoolServer poolServer, Map opts) {
        def rtn = new ServiceResponse()
        try {
            if(poolServer) {
                refresh(poolServer)
                rtn.data = poolServer
            } else {
                rtn.error = 'No pool server found'
            }
        } catch(e) {
            rtn.error = "initializeNetworkPoolServer error: ${e}"
            log.error("initializeNetworkPoolServer error: ${e}", e)
        }
        return rtn
    }

    /**
     * Creates a Host record on the target {@link NetworkPool} within the {@link NetworkPoolServer} integration.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp The ip address and metadata related to it for allocation. It is important to create functionality such that
     *                      if the ip address property is blank on this record, auto allocation should be performed and this object along with the new
     *                      ip address be returned in the {@link ServiceResponse}
     * @param domain The domain with which we optionally want to create an A/PTR record for during this creation process.
     * @param createARecord configures whether or not the A record is automatically created
     * @param createPtrRecord configures whether or not the PTR record is automatically created
     * @return a ServiceResponse containing the success state of the create host record operation
     */
    @Override
    ServiceResponse createHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp, NetworkDomain domain, Boolean createARecord, Boolean createPtrRecord) {
        HttpApiClient client = new HttpApiClient();
        try {
            String hostname = networkPoolIp.hostname
            if(domain && hostname && !hostname.endsWith(domain.name))  {
                hostname = "${hostname}.${domain.name}"
            }
            def tokenResults = getToken(client,poolServer)
            if(tokenResults.success) {
                String token = tokenResults.data.token as String
                if(networkPoolIp.ipAddress) {
                    // create requested IP
                    // POST /addresses?subnetId=
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL:poolServer.ignoreSsl, queryParams: [:])
                    requestOptions.queryParams.subnetId = networkPool.externalId
                    requestOptions.queryParams.hostname = hostname
                    requestOptions.queryParams.ip = networkPoolIp.ipAddress

                    def body = [:]
                    if(networkPoolIp.macAddress) {
                        requestOptions.queryParams.mac = networkPoolIp.macAddress
                    }
                    def createIpResults = callApi(client, poolServer.serviceUrl, 'addresses', getAppId(poolServer), token, requestOptions, 'POST')
                    if (!createIpResults.success) {
                        return createIpResults
                    }
                    networkPoolIp.externalId = createIpResults.data.id.toString()
                } else {
                    // create next free IP
                    // POST /addresses/first_free?subnetId=
                    HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(ignoreSSL:poolServer.ignoreSsl, queryParams: [:])
                    requestOptions.queryParams = [id: "first_free"]
                    requestOptions.queryParams.subnetId = networkPool.externalId
                    requestOptions.queryParams.hostname = hostname


                    def createFreeIpResults = callApi(client,poolServer.serviceUrl, 'addresses', getAppId(poolServer), token, requestOptions, 'POST')
                    if (!createFreeIpResults.success) {
                        return createFreeIpResults
                    }
                    // results.results == [id: N, data: "x.x.x.x"]
                    networkPoolIp.externalId = createFreeIpResults.data.id.toString()
                    networkPoolIp.ipAddress = createFreeIpResults.data.data
                }
            }

            if (networkPoolIp.id) {
                networkPoolIp = morpheus.network.pool.poolIp.save(networkPoolIp)?.blockingGet()
            } else {
                networkPoolIp = morpheus.network.pool.poolIp.create(networkPoolIp)?.blockingGet()
            }
            return ServiceResponse.success(networkPoolIp)
        } finally {
            client.shutdownClient()
        }
    }

    /**
     * Updates a Host record on the target {@link NetworkPool} if supported by the Provider. If not supported, send the appropriate
     * {@link ServiceResponse} such that the user is properly informed of the unavailable operation.
     * @param poolServer The Integration Object contains all the saved information regarding configuration of the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param networkPoolIp the changes to the network pool ip address that would like to be made. Most often this is just the host record name.
     * @return a ServiceResponse containing the success state of the update host record operation
     */
    @Override
    ServiceResponse updateHostRecord(NetworkPoolServer poolServer, NetworkPool networkPool, NetworkPoolIp networkPoolIp) {
        HttpApiClient client = new HttpApiClient();
        try {
            def tokenResults = getToken(client,poolServer)
            if(tokenResults.success) {
                String token = tokenResults.data.token as String
                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions([queryParams: [id:networkPoolIp.externalId, hostname: networkPoolIp.hostname], ignoreSSL: poolServer.ignoreSsl])
                def results = callApi(client, poolServer.serviceUrl, 'addresses', getAppId(poolServer), token, requestOptions, 'PATCH')
                if(results.success) {
                    return ServiceResponse.success(networkPoolIp)
                } else {
                    def msg = results.data?.message ?: results.msg ?: results.error ?: "Error Updating Host Record"
                    return ServiceResponse.error(msg, null, networkPoolIp)
                }
            }
        } finally {
            client.shutdownClient()
        }
    }

    /**
     * Deletes a host record on the target {@link NetworkPool}. This is used for cleanup or releasing of an ip address on
     * the IPAM Provider.
     * @param networkPool the NetworkPool currently being operated on.
     * @param poolIp the record that is being deleted.
     * @param deleteAssociatedRecords determines if associated records like A/PTR records
     * @return a ServiceResponse containing the success state of the delete operation
     */
    @Override
    ServiceResponse deleteHostRecord(NetworkPool networkPool, NetworkPoolIp poolIp, Boolean deleteAssociatedRecords) {
        HttpApiClient client = new HttpApiClient();
        def poolServer = morpheus.network.getPoolServerById(networkPool.poolServer.id).blockingGet()
        try {
            def tokenResults = getToken(client,poolServer)
            if(tokenResults.success) {
                String token = tokenResults.data.token as String

                HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions([queryParams: [id:poolIp.externalId], ignoreSSL: poolServer.ignoreSsl])
                def results = callApi(client,poolServer.serviceUrl, 'addresses', getAppId(poolServer), token, requestOptions, 'DELETE')
                if(results.success) {
                    return ServiceResponse.success(poolIp)
                } else {
                    def msg = results.data?.message ?: results.msg ?: results.error ?: "Error Deleting Host Record"
                    return ServiceResponse.error(msg, null, poolIp)
                }
            } else {
                return ServiceResponse.error("Unable to authenticate with {php}IPAM during host record delete! Please verify credentials are up to date for ${poolServer.name}.")
            }
        } finally {
            client.shutdownClient()
        }
    }

    /**
     * An IPAM Provider can register pool types for display and capability information when syncing IPAM Pools
     * @return a List of {@link NetworkPoolType} to be loaded into the Morpheus database.
     */
    @Override
    Collection<NetworkPoolType> getNetworkPoolTypes() {
        return [
                new NetworkPoolType(code:'phpipam', name:'phpIPAM', creatable:false, description:'phpIPAM', rangeSupportsCidr: false),
                new NetworkPoolType(code:'phpipamipv6', name:'phpIPAM IPv6', creatable:false, description:'phpIPAM IPv6', rangeSupportsCidr: true, ipv6Pool:true)
        ]
    }

    /**
     * Provide custom configuration options when creating a new {@link AccountIntegration}
     * @return a List of OptionType
     */
    @Override
    List<OptionType> getIntegrationOptionTypes() {
        return [
                new OptionType(code: 'networkPoolServer.phpipam.serviceUrl', name: 'Service URL', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUrl', fieldLabel: 'API Url', fieldContext: 'domain', helpBlock: 'gomorpheus.help.serviceUrl', required: true, displayOrder: 0),
                new OptionType(code: 'networkPoolServer.phpipam.appId', name: 'App ID', inputType: OptionType.InputType.TEXT, fieldName: 'appId', required: true, fieldLabel: 'App ID', fieldContext: 'config', displayOrder: 1),
                new OptionType(code: 'networkPoolServer.phpipam.credentials', name: 'Credentials', inputType: OptionType.InputType.CREDENTIAL, fieldName: 'type', fieldLabel: 'Credentials', fieldContext: 'credential', required: true, displayOrder: 2, defaultValue: 'local',optionSource: 'credentials',config: '{"credentialTypes":["username-password"]}'),
                new OptionType(code: 'networkPoolServer.phpipam.serviceUsername', name: 'Service Username', inputType: OptionType.InputType.TEXT, fieldName: 'serviceUsername', fieldLabel: 'Username', fieldContext: 'domain', displayOrder: 3, localCredential: true, required:true),
                new OptionType(code: 'networkPoolServer.phpipam.servicePassword', name: 'Service Password', inputType: OptionType.InputType.PASSWORD, fieldName: 'servicePassword', fieldLabel: 'Password', fieldContext: 'domain', displayOrder: 4, localCredential: true, required:true),
                new OptionType(code: 'networkPoolServer.phpipam.throttleRate', name: 'Throttle Rate', inputType: OptionType.InputType.NUMBER, defaultValue: 0, fieldName: 'serviceThrottleRate', fieldLabel: 'Throttle Rate', fieldContext: 'domain', displayOrder: 5),
                new OptionType(code: 'networkPoolServer.phpipam.ignoreSsl', name: 'Ignore SSL', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'ignoreSsl', fieldLabel: 'Disable SSL SNI Verification', fieldContext: 'domain', displayOrder: 6),
                new OptionType(code: 'networkPoolServer.phpipam.inventoryExisting', name: 'Inventory Existing', inputType: OptionType.InputType.CHECKBOX, defaultValue: 0, fieldName: 'inventoryExisting', fieldLabel: 'Inventory Existing', fieldContext: 'config', displayOrder: 7),
                new OptionType(code: 'networkPoolServer.phpipam.networkFilter', name: 'Network Filter', inputType: OptionType.InputType.TEXT, fieldName: 'networkFilter', fieldLabel: 'Network Filter', fieldContext: 'domain', displayOrder: 8)

        ]
    }

    /**
     * Returns the IPAM Integration logo for display when a user needs to view or add this integration
     * @since 0.12.3
     * @return Icon representation of assets stored in the src/assets of the project.
     */
    @Override
    Icon getIcon() {
        return new Icon(path:"phpipam-black.svg", darkPath: "phpipam-white.svg")
    }

/**
     * Returns the Morpheus Context for interacting with data stored in the Main Morpheus Application
     *
     * @return an implementation of the MorpheusContext for running Future based rxJava queries
     */
    @Override
    MorpheusContext getMorpheus() {
        return morpheusContext
    }

    /**
     * Returns the instance of the Plugin class that this provider is loaded from
     * @return Plugin class contains references to other providers
     */
    @Override
    Plugin getPlugin() {
        return plugin
    }

    /**
     * A unique shortcode used for referencing the provided provider. Make sure this is going to be unique as any data
     * that is seeded or generated related to this provider will reference it by this code.
     * @return short code string that should be unique across all other plugin implementations.
     */
    @Override
    String getCode() {
        return "phpipam"
    }

    /**
     * Provides the provider name for reference when adding to the Morpheus Orchestrator
     * NOTE: This may be useful to set as an i18n key for UI reference and localization support.
     *
     * @return either an English name of a Provider or an i18n based key that can be scanned for in a properties file.
     */
    @Override
    String getName() {
        return "phpIPAM"
    }


    /**
     * TODO: When implementing DNS for {php}IPAM
     * @param client
     * @param token
     * @param poolServer
     * @param zoneName
     * @param recordType
     * @param opts
     * @return
     */
    def listZoneRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, String zoneName, String recordType = 'record:a', Map opts=[:]) {
        return [success:false, results: [], msg: 'not yet implemented']
    }

    def listHostRecords(HttpApiClient client, String token, NetworkPoolServer poolServer, String subnetId) {

        def rtn = [success:false, addresses:[]]
        // GET /subnets/{id}/addresses
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions()
        requestOptions.queryParams = [id:subnetId, id2: 'addresses']
        requestOptions.ignoreSSL = poolServer.ignoreSsl
		def appId = getAppId(poolServer)
        def results = callApi(client, poolServer.serviceUrl, 'subnets', appId, token, requestOptions, 'GET')
        rtn.success = results.success
        if(rtn.success) {
            // if there are none, data is not returned at all, instead of an empty array
            def addressList = results.data.data ?: []
            rtn.addresses = addressList
        } else {
            rtn.msg = results?.error
        }
        return rtn
    }

    def listNetworks(HttpApiClient client, String token, NetworkPoolServer poolServer) {
        def rtn = [success:false, networks:[]]
        // this returns all of em by default
        // /api?app_id=jdtest&controller=subnets&id=all
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions()
        requestOptions.ignoreSSL = true

        def results = callApi(client,poolServer.serviceUrl, 'subnets', getAppId(poolServer), token, requestOptions, 'GET')
        rtn.success = results.success
        if(rtn.success) {
            def networkList = results.data.data
            // exclude ipv6 for now
            networkList = networkList?.findAll { it.subnet && it.mask && it.subnet != '0.0.0.0' } ?: []
            if (poolServer.networkFilter) {
                // apply filter, case insensitive match of entire description
                def poolNameFilter =  poolServer.networkFilter.tokenize(',').collect { it?.trim() }.findAll() { it }
                if (poolNameFilter.size() > 0) {
                    networkList = networkList.findAll { it ->
                        it.description && poolNameFilter.find { filterValue -> filterValue.toLowerCase() == it.description.toLowerCase() }
                    }
                }
            }
            rtn.networks = networkList
        } else {
            rtn.msg = results?.error
        }
        //println "listNetworks() api results : ${rtn}"
        return rtn
    }

    /**
     * Lists all the DNS Zones in {php}IPAM for consumption by Morpheus
     * @param client the HttpApiClient session for calling the API
     * @param poolServer current NetworkPoolServer record
     * @return
     */
    def listZones(HttpApiClient client, String token, NetworkPoolServer poolServer) {
        def rtn = [success:false, zones:[]]
        def apiQuery = [:]
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions([queryParams: [:], ignoreSSL: poolServer.ignoreSsl])
        def results = callApi(client, poolServer.serviceUrl, 'l2domains', getAppId(poolServer), token, requestOptions, 'GET')
        rtn.success = results.success

        if(rtn.success) {
            def domainList = results.data.data
            // exclude this default one..maybe?
            domainList = domainList?.findAll { it.name && it.name != 'default' } ?: []
            if (poolServer.networkFilter) {
                // todo: apply filter
            }
            rtn.zones = domainList
        } else {
            rtn.msg = results?.error
        }

        //println "listZones() api results : ${rtn}"
        return rtn
    }

    // fetch an auth token for use with callApi()
    ServiceResponse getToken(HttpApiClient client, NetworkPoolServer poolServer) {
        def token
        if (poolServer.configMap?.passwordToken) {
            token = poolServer.configMap?.passwordToken
        } else {
            def loginResult = login(client,poolServer)
            if (!loginResult.success) {
                return loginResult
            }
            token = loginResult.data.token
            // uncomment to start caching this token,
            // callApi() needs to know to re-auth and retry once for a 401...
            //poolServer.setConfigProperty('passwordToken', token)
            //poolServer.save(flush:true)
        }
        return ServiceResponse.success([token: token])
    }

    // login returns a ServiceResponse containing the token to use with callApi()
    // See https://phpipam.net/api-documentation/
    ServiceResponse login(HttpApiClient client, NetworkPoolServer poolServer) {
        //credentials

        def appId = getAppId(poolServer)
        HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(queryParams: [:])
        def creds = "${poolServer.credentialData?.username ?: poolServer.serviceUsername}:${poolServer.credentialData?.password ?: poolServer.servicePassword}"
        requestOptions.headers = ['Authorization': "Basic ${creds.getBytes().encodeBase64().toString()}".toString()]

        def apiResult = callApi(client, poolServer.serviceUrl, 'user', getAppId(poolServer), null, requestOptions, 'POST')
        log.info("PHP IPAM LOGIN REQUEST: ${apiResult}")
        if (apiResult.success) {
            if (!apiResult?.data?.data?.token) {
                return ServiceResponse.error("Failed to parse token from authorization response")
            }
            return ServiceResponse.success([token: apiResult.data.data?.token, expires: apiResult.data.data?.expires])
        } else {
            // return message from api itself
            if (apiResult?.data?.message) {
                return ServiceResponse.error(apiResult?.data?.message)
            } else {
                return ServiceResponse.error(apiResult.msg ?: apiResult.error ?: "phpIPAM api authentication request failed with error code ${apiResult.errorCode}")
            }
        }
    }

    /**
     * Used to logout of the HTTP API Session after the operation is complete
     * @param opts
     * @param token
     * @return
     */
    def logout(HttpApiClient client, token) {
        def rtn = [success:false]
        try {
            // todo
        } catch(e) {
            log.error("logout error: ${e}", e)
        }
        // return rtn
        if (rtn.success) {
            return ServiceResponse.success()
        } else {
            return ServiceResponse.error(rtn.msg ?: rtn.error ?: rtn.errorCode ?: "phpipam logout failed")
        }
    }


    /**
     * PHP IPAM expects the request urls to be in the following format:
     * <HTTP_METHOD> /api/<APP_NAME>/<CONTROLLER>/ HTTP/1.1
     * So the serviceUrl should contain the full path, including the app name.
     * @param client the HttpApiClient used for keep-alive sessions
     * @param url the api url of the php ipam endpoint
     * @param path the controller path (note this does not end up being the actual url path and is passed as a controller query param
     * @param appId the appId being referenced
     * @param token the authentication token
     * @param requestOptions any request options such as query parameters or body content
     * @param method the HTTP Method to be used i.e. GET,POST,PUT,DELETE
     * @return a ServiceResponse with data containing the JSON deserialized
     */
    ServiceResponse callApi(HttpApiClient client, String url, String path, String appId, String token, HttpApiClient.RequestOptions requestOptions = new HttpApiClient.RequestOptions(), String method = 'GET') {
        def rtn = [success: false, headers: [:]]
        // phpipam expects trailing slash for api calls eg. /api/?app_id=
        if (url && !url.endsWith("/")) {
            url = url + "/"
        }
        //log.info("callApi(${url}, ${path}, ${appId}, ${token}, ${opts}, ${method})")
        if(token) {
            requestOptions.headers = requestOptions.headers ?: [:]
            requestOptions.headers['phpipam-token'] = "${token}".toString()
        }

        requestOptions.queryParams = requestOptions.queryParams ?: [:]
        if(appId) {
            requestOptions.queryParams['app_id'] = appId.toString()
        }
        requestOptions.queryParams['controller'] =  path.toString()
        return client.callJsonApi(url,null,null,null,requestOptions,method)
    }


    // network filtering only supports a list of comma separated names.
    // if present, only networks with a name in the list will be synced.
    private String[] parseNetworkFilter(networkFilter) {
        return networkFilter.tokenize(',').collect { it?.trim() }.findAll { it } ?: []
    }

    private String getServiceUrl(NetworkPoolServer poolServer) {
        def url = poolServer.serviceUrl
        // don't do anything to it
        return poolServer.serviceUrl
    }
    private String getAppId(NetworkPoolServer poolServer) {
        return poolServer?.configMap?.appId
    }

    private static Map getNetworkPoolConfig(cidr) {
        def rtn = [config:[:], ranges:[]]
        try {
            def subnetInfo = new SubnetUtils(cidr).getInfo()
            rtn.config.netmask = subnetInfo.getNetmask()
            rtn.config.ipCount = subnetInfo.getAddressCountLong() ?: 0
            rtn.config.ipFreeCount = rtn.config.ipCount
            rtn.ranges << [startAddress:subnetInfo.getLowAddress(), endAddress:subnetInfo.getHighAddress()]
        } catch(e) {
            log.warn("error parsing network pool cidr: ${e}", e)
        }
        return rtn
    }


}
