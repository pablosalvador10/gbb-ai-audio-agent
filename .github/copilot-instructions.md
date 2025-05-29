# GitHub Copilot Instructions for RTAgent Voice AI Backend

## 🎯 Project Overview
This is a Voice AI Agent backend built with FastAPI, Azure Communication Services, and Azure OpenAI. The project follows enterprise-grade patterns for secure, scalable, and maintainable cloud-native applications.

## 🔐 Security & Authentication Guidelines

### Authentication Patterns
- **ALWAYS** use Managed Identity for Azure-hosted services
- **NEVER** hardcode credentials or API keys in source code
- Use Azure Key Vault for all secrets and configuration
- Implement Service Principal for CI/CD pipelines
- Use Interactive Browser authentication for user-facing applications
- Implement proper credential rotation and least privilege principles

### Secure Coding Standards
```python
# ✅ GOOD: Use Managed Identity
from azure.identity import DefaultAzureCredential
credential = DefaultAzureCredential()

# ❌ BAD: Hardcoded credentials
api_key = "your-secret-key-here"  # Never do this
```

### Data Protection
- Enable encryption at rest and in transit for all Azure services
- Use TLS 1.2+ for all external communications
- Implement proper input validation and sanitization
- Use parameterized queries for database operations
- Apply data classification and retention policies

## 🏗️ Infrastructure as Code (IaC) Standards

### Azure Well-Architected Framework (WAF) Principles
Follow the five pillars when designing infrastructure:

1. **Reliability**: Implement high availability, disaster recovery, and fault tolerance
2. **Security**: Apply defense-in-depth, zero-trust architecture
3. **Cost Optimization**: Right-size resources, use reserved instances, monitor spending
4. **Operational Excellence**: Automate deployments, implement monitoring
5. **Performance Efficiency**: Scale resources based on demand, optimize for performance

### Cloud Adoption Framework (CAF) Alignment
- Use standardized naming conventions (`<resource-type>-<workload>-<environment>-<region>-<instance>`)
- Implement proper resource tagging strategy
- Follow enterprise-scale landing zone patterns
- Apply governance and compliance policies

### Bicep Best Practices
```bicep
// ✅ GOOD: Parameterized, secure, and documented
@description('The environment name (dev, test, prod)')
@allowed(['dev', 'test', 'prod'])
param environmentName string

@description('The Azure region for resources')
param location string = resourceGroup().location

@secure()
@description('Database administrator password')
param dbAdminPassword string

// Use consistent naming with resource tokens
var resourceToken = uniqueString(resourceGroup().id)
var appServiceName = 'app-rtvoice-${environmentName}-${location}-${resourceToken}'

resource appService 'Microsoft.Web/sites@2023-01-01' = {
  name: appServiceName
  location: location
  identity: {
    type: 'SystemAssigned'  // Always use Managed Identity
  }
  properties: {
    httpsOnly: true  // Enforce HTTPS
    // ... other properties
  }
  tags: {
    Environment: environmentName
    Application: 'RTVoiceAgent'
    CostCenter: 'Engineering'
    Owner: 'Platform-Team'
  }
}
```

### Infrastructure Organization
- Place all Bicep files in `/infra/` directory
- Use modular design with `/infra/modules/` for reusable components
- Implement proper parameter files for different environments
- Include comprehensive resource tagging

## 🚀 Application Architecture Patterns

### FastAPI Best Practices
```python
# ✅ GOOD: Proper lifespan management
from contextlib import asynccontextmanager
from fastapi import FastAPI

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize Azure services
    await initialize_azure_services(app)
    yield
    # Cleanup Azure services
    await cleanup_azure_services(app)

app = FastAPI(lifespan=lifespan)
```

### Error Handling & Resilience
- Implement exponential backoff for transient failures
- Use circuit breaker patterns for external service calls
- Add comprehensive logging with correlation IDs
- Include health check endpoints for monitoring
- Implement graceful degradation strategies

### Performance & Scaling
```python
# ✅ GOOD: Connection pooling and async patterns
import asyncio
from azure.storage.blob.aio import BlobServiceClient

class AzureStorageManager:
    def __init__(self):
        self.credential = DefaultAzureCredential()
        self.blob_client = BlobServiceClient(
            account_url="https://storage.blob.core.windows.net",
            credential=self.credential,
            max_pool_connections=20  # Configure connection pooling
        )
    
    async def upload_batch(self, files: list):
        # Use async operations for better performance
        tasks = [self._upload_file(file) for file in files]
        await asyncio.gather(*tasks)
```

## 📊 Monitoring & Observability

### Logging Standards
- Use structured logging with JSON format
- Include correlation IDs for request tracing
- Log security events and access patterns
- Implement log aggregation with Azure Monitor

### Application Insights Integration
```python
# ✅ GOOD: Proper telemetry configuration
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.ext.azure import trace_exporter

# Configure Application Insights
app_insights_key = get_secret_from_keyvault("app-insights-key")
trace_exporter = trace_exporter.AzureExporter(
    connection_string=f"InstrumentationKey={app_insights_key}"
)
```

## 🔧 Development Workflow

### Git Practices
- Use conventional commit messages
- Implement branch protection rules
- Require pull request reviews
- Run security scans on all commits

### CI/CD Pipeline Standards
- Use GitHub Actions with Azure integration
- Implement infrastructure validation (bicep lint, what-if)
- Run security scans (CodeQL, Dependabot)
- Deploy through staging environments first
- Use blue-green or canary deployment strategies

### Testing Requirements
- Unit tests with >80% coverage
- Integration tests for Azure services
- Load testing for performance validation
- Security testing for vulnerability assessment

## 🛠️ Azure MCP Server Integration

### When to Use Azure MCP
- For querying Azure data plane resources (Cosmos DB, Storage, etc.)
- Prefer `azmcp` commands over `az cli` for data operations
- Use for real-time monitoring and diagnostics
- Implement for automated troubleshooting scenarios

### Example Usage Patterns
```bash
# ✅ GOOD: Use Azure MCP for data plane operations
azmcp cosmos query --database mydb --collection mycollection --query "SELECT * FROM c"

# ✅ GOOD: Use for storage operations
azmcp storage blob list --account myaccount --container mycontainer

# ❌ Avoid: Using az cli for data plane when MCP is available
az cosmosdb sql query  # Use azmcp instead
```

## 📋 Code Review Checklist

### Security Review
- [ ] No hardcoded secrets or credentials
- [ ] Proper authentication mechanisms implemented
- [ ] Input validation and sanitization applied
- [ ] Encryption enabled for sensitive data
- [ ] RBAC and least privilege principles followed

### Architecture Review
- [ ] Follows established patterns and conventions
- [ ] Implements proper error handling and retry logic
- [ ] Uses async patterns where appropriate
- [ ] Includes comprehensive logging and monitoring
- [ ] Follows separation of concerns principles

### Infrastructure Review
- [ ] Bicep templates follow WAF and CAF principles
- [ ] Proper resource naming and tagging implemented
- [ ] Security configurations enabled (HTTPS, encryption)
- [ ] Cost optimization measures applied
- [ ] Monitoring and alerting configured

## 🎨 Naming Conventions

### Resource Naming
- **Format**: `<resource-type>-<workload>-<environment>-<region>-<instance>`
- **Examples**:
  - `app-rtvoice-prod-eastus2-001`
  - `kv-rtvoice-dev-westus2-001`
  - `redis-rtvoice-test-northeurope-001`

### Code Naming
- Use descriptive, self-documenting names
- Follow Python PEP 8 conventions
- Use consistent terminology across the codebase
- Avoid abbreviations unless industry standard

## 🔄 Deployment Strategy

### Environment Progression
1. **Development**: Feature development and unit testing
2. **Testing**: Integration testing and QA validation
3. **Staging**: Production-like environment for final validation
4. **Production**: Live environment with monitoring and alerting

### Deployment Validation
```bash
# Always validate before deployment
azd provision --preview  # For azd deployments
az deployment group what-if  # For direct ARM/Bicep deployments

# Validate post-deployment
azd logs  # Check application logs
azmcp monitor health  # Check service health
```

## 🎯 Performance Targets

### Application Performance
- API response time: < 500ms (95th percentile)
- Database query time: < 100ms (average)
- Memory usage: < 80% of allocated resources
- CPU utilization: < 70% under normal load

### Scalability Requirements
- Support for 1000+ concurrent users
- Auto-scaling based on CPU/memory metrics
- Horizontal scaling capability
- Load balancing across multiple instances

## 📚 Documentation Standards

### Code Documentation
- Include docstrings for all public methods
- Add inline comments for complex business logic
- Document API endpoints with OpenAPI specifications
- Maintain up-to-date README files

### Infrastructure Documentation
- Document architecture decisions and rationale
- Include deployment guides and troubleshooting steps
- Maintain infrastructure diagrams and dependencies
- Document disaster recovery procedures

---

**Remember**: Security and scalability should be considered from the beginning, not added as an afterthought. Always prioritize user data protection and system reliability in every design decision.
