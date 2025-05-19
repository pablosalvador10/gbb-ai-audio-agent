@description('The location for all resources')
param location string = resourceGroup().location

@description('Name of the Virtual Network')
param vnetName string = 'vnet-ai-audio-agent'

@description('Address space for the Virtual Network')
param vnetAddressPrefix string = '10.0.0.0/16'

@description('Subnet configuration')
type SubnetConfig = {
  name: string
  addressPrefix: string
}

@description('Load Balancer Subnet Configuration')
param loadBalancer SubnetConfig = {
  name: 'load-balancer'
  addressPrefix: '10.0.1.0/24'
}

@description('Application Subnet Configuration')
param appSubnet SubnetConfig = {
  name: 'app'
  addressPrefix: '10.0.2.0/24'
}

@description('Private Endpoint Subnet Configuration')
param privateEndpointSubnet SubnetConfig = {
  name: 'private-endpoint'
  addressPrefix: '10.0.3.0/24'
}

@description('Services Subnet Configuration')
param servicesSubnet SubnetConfig = {
  name: 'services'
  addressPrefix: '10.0.4.0/24'
}

@description('Domain label for the public IP address (<domainlabel>.<location>.cloudapp.azure.com)')
param pipDomainLabel string = toLower('rtaudio-${uniqueString(resourceGroup().id, vnetName)}')

var subnets = [
  loadBalancer
  appSubnet
  privateEndpointSubnet
  servicesSubnet
]

@description('Tags to apply to all resources')
param tags object = {}

@description('Enable creation of Private DNS Zone')
param enablePrivateDnsZone bool = false

@description('Private DNS Zone name')
param privateDnsZoneName string = 'privatelink.openai.azure.com'

resource vnet 'Microsoft.Network/virtualNetworks@2023-02-01' = {
  name: vnetName
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: [
        vnetAddressPrefix
      ]
    }
    subnets: [for subnet in subnets: {
      name: subnet.name
      properties: {
        addressPrefix: subnet.addressPrefix
      }
    }]
  }
}

var subnetResourceIds = [for subnet in subnets: resourceId('Microsoft.Network/virtualNetworks/subnets', vnetName, subnet.name)]

resource publicIp 'Microsoft.Network/publicIPAddresses@2023-02-01' = {
  name: 'pip-${vnetName}'
  location: location
  tags: tags
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
    dnsSettings: {
      domainNameLabel: pipDomainLabel
    }
  }
}


// Output the FQDN and IP address of the public IP for downstream consumption
// Output the FQDN, IP address, and resource ID of the public IP for downstream consumption
output publicIpFqdn string = publicIp.properties.dnsSettings.fqdn
output publicIpAddress string = publicIp.properties.ipAddress
output publicIpResourceId string = publicIp.id


resource privateDnsZone 'Microsoft.Network/privateDnsZones@2020-06-01' = if (enablePrivateDnsZone) {
  name: privateDnsZoneName
  location: 'global'
  tags: tags
}

// Output VNet and subnet details for downstream modules
output vnetId string = vnet.id
output vnetName string = vnet.name
output subnetResourceIds array = subnetResourceIds
output subnetNames array = [for subnet in subnets: subnet.name]
output appgwSubnetResourceId string = subnetResourceIds[0]
output backendSubnetResourceId string = subnetResourceIds[1]
output privateEndpointSubnetResourceId string = subnetResourceIds[2]
output servicesSubnetResourceId string = subnetResourceIds[3]
output privateDnsZoneId string = enablePrivateDnsZone ? privateDnsZone.id : ''

