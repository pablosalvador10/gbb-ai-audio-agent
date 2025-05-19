@description('The location used for all deployed resources')
param location string = resourceGroup().location

@description('Tags that will be applied to all resources')
param tags object = {}

@description('Name of the environment that can be used as part of naming resource convention')
param name string

// AZD managed variables
param gbbAiAudioAgentExists bool
param gbbAiAudioAgentBackendExists bool
param acsSourcePhoneNumber string = ''

param enableEasyAuth bool = true
param appInsightsConnectionString string = 'InstrumentationKey=00000000-0000-0000-0000-000000000000;IngestionEndpoint=https://dc.services.visualstudio.com/v2/track'
param logAnalyticsWorkspaceResourceId string = '00000000-0000-0000-0000-000000000000'

// Network parameters for reference
param vnetName string
param appgwSubnetResourceId string
param appSubnetResourceId string

@description('Id of the user or app to assign application roles')
param principalId string

var abbrs = loadJsonContent('./abbreviations.json')
var resourceToken = uniqueString(subscription().id, resourceGroup().id, location)

// Container registry
module containerRegistry 'br/public:avm/res/container-registry/registry:0.1.1' = {
  name: 'registry'
  params: {
    name: '${name}${abbrs.containerRegistryRegistries}${resourceToken}'
    location: location
    tags: tags
    publicNetworkAccess: 'Enabled'
    roleAssignments:[
      {
        principalId: frontendUserAssignedIdentity.outputs.principalId
        principalType: 'ServicePrincipal'
        roleDefinitionIdOrName: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
      }
      {
        principalId: backendUserAssignedIdentity.outputs.principalId
        principalType: 'ServicePrincipal'
        roleDefinitionIdOrName: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '7f951dda-4ed3-4680-a7ca-43fe172d538d')
      }
    ]
  }
}

// Container apps environment (deployed into appSubnet)
module containerAppsEnvironment 'br/public:avm/res/app/managed-environment:0.11.1' = {
  name: 'container-apps-environment'
  params: {
    logAnalyticsWorkspaceResourceId: logAnalyticsWorkspaceResourceId
    name: '${name}${abbrs.appManagedEnvironments}${resourceToken}'
    location: location
    zoneRedundant: false
    infrastructureSubnetId: appSubnetResourceId // Enables private networking in the specified subnet
    internal: appSubnetResourceId != '' ? true : false
    tags: tags
    certificate: {
      certificateKeyVaultProperties: {
        identityResourceId: '<identityResourceId>'
        keyVaultUrl: '<keyVaultUrl>'
      }
      name: 'dep-cert-amemax'
    }
    certificatePassword: 
  }
}

module frontendUserAssignedIdentity 'br/public:avm/res/managed-identity/user-assigned-identity:0.2.1' = {
  name: 'gbbAiAudioAgentidentity'
  params: {
    name: '${name}${abbrs.managedIdentityUserAssignedIdentities}gbbAiAudioAgent-${resourceToken}'
    location: location
  }
}
module backendUserAssignedIdentity 'br/public:avm/res/managed-identity/user-assigned-identity:0.2.1' = {
  name: 'gbbAiAudioAgentBackendIdentity'
  params: {
    name: '${name}${abbrs.managedIdentityUserAssignedIdentities}gbbAiAudioAgentBackend-${resourceToken}'
    location: location
  }
}

// Azure Communication Services resource (placed in servicesSubnet if supported)
resource communicationServices 'Microsoft.Communication/communicationServices@2023-04-01-preview' = {
  name: '${name}acs${resourceToken}'
  location: 'global'
  tags: tags
  properties: {
    dataLocation: 'UnitedStates' // Change as needed for your compliance requirements
    
  }
}

var beContainerName =  toLower(substring('rt-agent-be-${resourceToken}', 0, 25))
var feContainerName =  toLower(substring('rt-agent-${resourceToken}', 0, 22))

module fetchFrontendLatestImage './modules/app/fetch-container-image.bicep' = {
  name: 'gbbAiAudioAgent-fetch-image'
  params: {
    exists: gbbAiAudioAgentExists
    name: feContainerName
  }
}
module fetchBackendLatestImage './modules/app/fetch-container-image.bicep' = {
  name: 'gbbAiAudioAgentBackend-fetch-image'
  params: {
    exists: gbbAiAudioAgentBackendExists
    name: beContainerName
  }
}

module frontendAudioAgent 'modules/app/container-app.bicep' = {
  name: 'frontend-audio-agent'
  params: {
    name: feContainerName
    enableEasyAuth: enableEasyAuth
    publicAccessAllowed: true
    ingressTargetPort: 5173
    scaleMinReplicas: 1
    scaleMaxReplicas: 10
    stickySessionsAffinity: 'sticky'
    containers: [
      {
        image: fetchFrontendLatestImage.outputs.?containers[?0].?image ?? 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'
        name: 'main'
        resources: {
          cpu: json('0.5')
          memory: '1.0Gi'
        }
        env: [
          {
            name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
            value: appInsightsConnectionString
          }
          {
            name: 'AZURE_CLIENT_ID'
            value: frontendUserAssignedIdentity.outputs.clientId
          }
          {
            name: 'PORT'
            value: '5173'
          }
          {
            name: 'VITE_BACKEND_BASE_URL'
            value: 'https://${existingAppGatewayPublicIp.properties.dnsSettings.fqdn}'
          }
        ]
      }
    ]
    userAssignedResourceId: frontendUserAssignedIdentity.outputs.resourceId
    registries:[
      {
        server: containerRegistry.outputs.loginServer
        identity: frontendUserAssignedIdentity.outputs.resourceId
      }
    ]
    environmentResourceId: containerAppsEnvironment.outputs.resourceId
    location: location
    tags: union(tags, { 'azd-service-name': 'gbb-ai-audio-agent' })
  }
}

@description('Resource ID of an existing Application Gateway to use')
// param existingAppGatewayResourceName string = 'ai-realtime-sandbox-wus2-appgw'
param existingAppGatewayResourceGroupName string = 'ai-realtime-sandbox'

// resource existingAppGateway 'Microsoft.Network/applicationGateways@2022-09-01' existing = if (!empty(existingAppGatewayResourceName)) {
//   scope: resourceGroup(existingAppGatewayResourceGroupName)
//   name: existingAppGatewayResourceName
// }

@description('Name of the existing public IP address associated with the Application Gateway')
param existingAppGatewayPublicIpName string = 'ai-realtime-sandbox-appgw-pip'

resource existingAppGatewayPublicIp 'Microsoft.Network/publicIPAddresses@2022-05-01' existing = if (!empty(existingAppGatewayPublicIpName)) {
  scope: resourceGroup(existingAppGatewayResourceGroupName)
  name: existingAppGatewayPublicIpName
}

module backendAudioAgent './modules/app/container-app.bicep' = {
  name: 'gbbAiAudioAgentBackendApp'
  params: {
    name: beContainerName
    ingressTargetPort: 8010
    scaleMinReplicas: 1
    scaleMaxReplicas: 10
    secrets: []
    containers: [
      {
        image: fetchBackendLatestImage.outputs.?containers[?0].?image ?? 'mcr.microsoft.com/azuredocs/containerapps-helloworld:latest'
        name: 'main'
        resources: {
          cpu: json('1.0')
          memory: '2.0Gi'
        }
        env: [
          {
            name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
            value: monitoring.outputs.applicationInsightsConnectionString
          }
          {
            name: 'AZURE_CLIENT_ID'
            value: backendUserAssignedIdentity.outputs.clientId
          }
          {
            name: 'PORT'
            value: '8010'
          }
          {
            name: 'AZURE_OPENAI_ENDPOINT'
            value: aiGateway.outputs.aiServices[0].endpoints.value['OpenAI Language Model Instance API']
          }
          {
            name: 'AZURE_OPENAI_CHAT_DEPLOYMENT_ID'
            value: backendConfig[0].?chat.name
          }
          {
            name: 'AZURE_SPEECH_RESOURCE_ID'
            value: aiGateway.outputs.aiServicesIds[0]
          }
          {
            name: 'AZURE_SPEECH_REGION'
            value: location
          }
          {
            name: 'BASE_URL'
            value: 'https://${existingAppGatewayPublicIp.properties.ipAddress}'
          }
          {
            name: 'ACS_SOURCE_PHONE_NUMBER'
            value: acsSourcePhoneNumber
          }
          {
            name: 'ACS_RESOURCE_ENDPOINT'
            value: 'https://${communicationServices.properties.hostName}'
          }
          {
            name: 'USE_ENTRA_CREDENTIALS'
            value: 'true'
          }
        ]
      }
    ]
    userAssignedResourceId: backendUserAssignedIdentity.outputs.resourceId
    registries: [
      {
        server: containerRegistry.outputs.loginServer
        identity: backendUserAssignedIdentity.outputs.resourceId
      }
    ]
    environmentResourceId: containerAppsEnvironment.outputs.resourceId
    location: location
    tags: union(tags, { 'azd-service-name': 'gbb-ai-audio-agent-backend' })
  }
}

resource aiDeveloperRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(backendUserAssignedIdentity.name, 'AI Developer')
  scope: resourceGroup()
  properties: {
    principalId: backendUserAssignedIdentity.outputs.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '64702f94-c441-49e6-a78b-ef80e0188fee') // AI Developer
    principalType: 'ServicePrincipal'
  }
}

resource cognitiveServicesContributorRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(backendUserAssignedIdentity.name, 'Cognitive Services Contributor')
  scope: resourceGroup()
  properties: {
    principalId: backendUserAssignedIdentity.outputs.principalId
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '25fbc0a9-bd7c-42a3-aa1a-3b75d497ee68') // Cognitive Services Contributor
    principalType: 'ServicePrincipal'
  }
}

// Outputs for downstream consumption and integration

// Container Registry
output containerRegistryEndpoint string = containerRegistry.outputs.loginServer
output containerRegistryResourceId string = containerRegistry.outputs.resourceId

// Container Apps Environment
output containerAppsEnvironmentId string = containerAppsEnvironment.outputs.resourceId

// User Assigned Identities
output frontendUserAssignedIdentityClientId string = frontendUserAssignedIdentity.outputs.clientId
output frontendUserAssignedIdentityResourceId string = frontendUserAssignedIdentity.outputs.resourceId
output backendUserAssignedIdentityClientId string = backendUserAssignedIdentity.outputs.clientId
output backendUserAssignedIdentityResourceId string = backendUserAssignedIdentity.outputs.resourceId

// Communication Services
output communicationServicesResourceId string = communicationServices.id
output communicationServicesEndpoint string = communicationServices.properties.hostName

// AI Gateway (ensure it is deployed into the servicesSubnet if required by module implementation)
output aiGatewayEndpoints array = aiGateway.outputs.aiServices
output aiGatewayServiceIds array = aiGateway.outputs.aiServicesIds

// Container Apps
output frontendContainerAppResourceId string = frontendAudioAgent.outputs.containerAppResourceId
output backendContainerAppResourceId string = backendAudioAgent.outputs.containerAppResourceId
output frontendAppName string = feContainerName
output backendAppName string = beContainerName

// Application Gateway Integration
output frontendBaseUrl string = 'https://${existingAppGatewayPublicIp.properties.dnsSettings.fqdn}'
output backendBaseUrl string = 'https://${existingAppGatewayPublicIp.properties.ipAddress}'



// NOTE: These parameters are currently not used directly in this file, but are available for future use and for passing to modules that support subnet assignment.
