

@description('Enable Application Gateway deployment')
param enableAppGateway bool = true

@description('Location for Application Gateway')
param location string

@description('Name of the Virtual Network')
param vnetName string

@description('Subnet resource IDs (from VNet module)')
param subnetResourceIds array

@description('Tags to apply to all resources')
param tags object = {}

@description('App Gateway SKU')
@allowed([
  'Standard_v2'
  'WAF_v2'
])
param appGatewaySku string = 'WAF_v2'

@description('Public IP resource ID for frontend')
param publicIpResourceId string

@description('Backend FQDN (container app)')
param backendFqdn string

@description('Base64-encoded SSL Root certificate (.CER)')
param sslCertBase64 string

@description('Health probe path')
param healthProbePath string = '/healthz'

resource appGateway 'Microsoft.Network/applicationGateways@2023-02-01' = if (enableAppGateway) {
  name: 'appgw-${vnetName}'
  location: location
  tags: tags
  zones: [ '1', '2', '3' ]
  properties: {
    sku: {
      name: appGatewaySku
      tier: appGatewaySku
    }
    gatewayIPConfigurations: [
      {
        name: 'appGatewayIpConfig'
        properties: {
          subnet: {
            id: subnetResourceIds[0]
          }
        }
      }
    ]
    frontendIPConfigurations: [
      {
        name: 'appGwPublicFrontendIpIPv4'
        properties: {
          privateIPAllocationMethod: 'Dynamic'
          publicIPAddress: {
            id: publicIpResourceId
          }
        }
      }
    ]
    frontendPorts: [
      {
        name: 'port_443'
        properties: { port: 443 }
      }
    ]
    sslCertificates: [
      {
        name: 'server'
        properties: {
          data: sslCertBase64
        }
      }
    ]
    backendAddressPools: [
      {
        name: 'rtaudio-backend-pool'
        properties: {
          backendAddresses: [
            {
              fqdn: backendFqdn
            }
          ]
        }
      }
    ]
    backendHttpSettingsCollection: [
      {
        name: 'rtaudio-https-setting'
        properties: {
          port: 443
          protocol: 'Https'
          cookieBasedAffinity: 'Disabled'
          connectionDraining: {
            enabled: true
            drainTimeoutInSec: 60
          }
          pickHostNameFromBackendAddress: true
          requestTimeout: 20
          probe: {
            id: 'probe-https'
          }
        }
      }
    ]
    probes: [
      {
        name: 'rtaudio-https-probe'
        properties: {
          protocol: 'Https'
          path: healthProbePath
          interval: 120
          timeout: 10
          unhealthyThreshold: 3
          pickHostNameFromBackendHttpSettings: true
          match: {
            body: ''
            statusCodes: [ '200-399' ]
          }
        }
      }
    ]
    httpListeners: [
      {
        name: 'rtaudio-https-listener'
        properties: {
          frontendIPConfiguration: {
            id: 'appGwPublicFrontendIpIPv4'
          }
          frontendPort: {
            id: 'port_443'
          }
          protocol: 'Https'
          sslCertificate: {
            id: 'server'
          }
          requireServerNameIndication: false
        }
      }
    ]
    urlPathMaps: [
      {
        name: 'rtaudio-pathbased-https-rule'
        properties: {
          defaultBackendAddressPool: {
            id: 'rtaudio-backend-pool'
          }
          defaultBackendHttpSettings: {
            id: 'rtaudio-https-setting'
          }
          pathRules: [
            {
              name: 'backend-apis-ws'
              properties: {
                paths: [ '/api/*', '/realtime-acs', '/realtime' ]
                backendAddressPool: {
                  id: 'rtaudio-backend-pool'
                }
                backendHttpSettings: {
                  id: 'rtaudio-https-setting'
                }
              }
            }
          ]
        }
      }
    ]
    requestRoutingRules: [
      {
        name: 'pathbased-https-rule'
        properties: {
          ruleType: 'PathBasedRouting'
          httpListener: {
            id: 'https-listener'
          }
          urlPathMap: {
            id: 'pathbased-https-rule'
          }
        }
      }
    ]
    webApplicationFirewallConfiguration: {
      enabled: false
      firewallMode: 'Detection'
      ruleSetType: 'OWASP'
      ruleSetVersion: '3.0'
      disabledRuleGroups: []
    }
    enableHttp2: true
  }
}
