targetScope = 'subscription'

@minLength(1)
@maxLength(64)
@description('Name of the environment that can be used as part of naming resource convention')
param environmentName string

param name string = 'rtaudioagent'

@minLength(1)
@description('Primary location for all resources')
param location string

import { ModelConfig, BackendConfigItem } from './modules/types.bicep'

param gbbAiAudioAgentExists bool
param gbbAiAudioAgentBackendExists bool
param acsSourcePhoneNumber string

@description('Flag to enable/disable the use of APIM for OpenAI loadbalancing')
param enableAPIManagement bool = true

@description('[Required when enableAPIManagement is true] Array of backend configurations for the AI services.')
param azureOpenAIBackendConfig BackendConfigItem[]

@description('Id of the user or app to assign application roles')
param principalId string

@secure()
@description('Base64-encoded Root SSL certificate (.cer) for Application Gateway')
param rootCertificateBase64Value string

var abbrs = loadJsonContent('./abbreviations.json')
var resourceToken = uniqueString(subscription().id, rg.id, location)


// Tags that should be applied to all resources.
// 
// Note that 'azd-service-name' tags should be applied separately to service host resources.
// Example usage:
//   tags: union(tags, { 'azd-service-name': <service name in azure.yaml> })
var tags = {
  'azd-env-name': environmentName
  'hidden-title': 'Real Time Audio ${environmentName}'

}

// Organize resources in a resource group
resource rg 'Microsoft.Resources/resourceGroups@2021-04-01' = {
  name: 'rg-${name}-${environmentName}'
  location: location
  tags: tags
}

// Monitor application with Azure Monitor
module monitoring 'br/public:avm/ptn/azd/monitoring:0.1.0' = {
  name: 'monitoring'
  scope: rg
  params: {
    logAnalyticsName: '${abbrs.operationalInsightsWorkspaces}${resourceToken}'
    applicationInsightsName: '${abbrs.insightsComponents}${resourceToken}'
    applicationInsightsDashboardName: '${abbrs.portalDashboards}${resourceToken}'
    location: location
    tags: tags
  }
}

module network 'network.bicep' = {
  scope: rg
  name: 'network'
  params: {
    location: location
    tags: tags
    vnetName: 'vnet-${name}-${environmentName}'
    vnetAddressPrefix: '10.0.0.0/16'
    // Optionally, you can pass custom subnet configs or domain label here if needed
  }
}


module aiGateway 'ai-gateway.bicep' = {
  scope: rg
  name: 'ai-gateway'
  params: {
    name: name
    enableAPIManagement: enableAPIManagement
    location: location
    tags: tags
    apimSku: 'StandardV2'
    backendConfig: azureOpenAIBackendConfig
    // Pass monitoring config from monitoring module
    diagnosticSettings: [
      {
        name: 'default'
        workspaceResourceId: monitoring.outputs.logAnalyticsWorkspaceResourceId
      }
    ]
  }
}


module app 'app.bicep' = {
  scope: rg
  name: 'app'
  params: {
    name: name
    location: location
    tags: tags

    // Monitoring
    appInsightsConnectionString: monitoring.outputs.applicationInsightsConnectionString
    logAnalyticsWorkspaceResourceId: monitoring.outputs.logAnalyticsWorkspaceResourceId
    // Managed by AZD to deploy code to container apps
    acsSourcePhoneNumber: acsSourcePhoneNumber
    gbbAiAudioAgentExists: gbbAiAudioAgentExists
    gbbAiAudioAgentBackendExists: gbbAiAudioAgentBackendExists

    // Network configuration from network module
    vnetName: network.outputs.vnetName
    appgwSubnetResourceId: network.outputs.appgwSubnetResourceId
    appSubnetResourceId: network.outputs.backendSubnetResourceId
  }
}

module loadbalancer 'loadbalancer.bicep' = {
  scope: rg
  name: 'loadbalancer'
  params: {
    location: location
    tags: tags
    vnetName: network.outputs.vnetName
    subnetResourceIds: network.outputs.subnetResourceIds
    enableAppGateway: false // Set to true if you want to enable Application Gateway
    appGatewaySku: 'Standard_v2'
    backendFqdn: resources.outputs.backendBaseUrl
    publicIpResourceId: network.outputs.publicIpResourceId
    sslCertBase64: rootCertificateBase64Value
  }
}

output containerRegistryEndpoint string = resources.outputs.containerRegistryEndpoint
output containerRegistryResourceId string = resources.outputs.containerRegistryResourceId
output containerAppsEnvironmentId string = resources.outputs.containerAppsEnvironmentId
output frontendUserAssignedIdentityClientId string = resources.outputs.frontendUserAssignedIdentityClientId
output frontendUserAssignedIdentityResourceId string = resources.outputs.frontendUserAssignedIdentityResourceId
output backendUserAssignedIdentityClientId string = resources.outputs.backendUserAssignedIdentityClientId
output backendUserAssignedIdentityResourceId string = resources.outputs.backendUserAssignedIdentityResourceId
output communicationServicesResourceId string = resources.outputs.communicationServicesResourceId
output communicationServicesEndpoint string = resources.outputs.communicationServicesEndpoint
output aiGatewayEndpoints array = resources.outputs.aiGatewayEndpoints
output aiGatewayServiceIds array = resources.outputs.aiGatewayServiceIds
output frontendContainerAppResourceId string = resources.outputs.frontendContainerAppResourceId
output backendContainerAppResourceId string = resources.outputs.backendContainerAppResourceId
output frontendAppName string = resources.outputs.frontendAppName
output backendAppName string = resources.outputs.backendAppName
output frontendBaseUrl string = resources.outputs.frontendBaseUrl
output backendBaseUrl string = resources.outputs.backendBaseUrl
