---
mode: 'ask'
---
Expected output and any relevant constraints for this task:

1. **Secure ReactJS Development**:
    - Ensure all React components follow secure coding practices.
    - Avoid exposing sensitive data in the frontend.
    - Use environment variables for sensitive configurations.
    - Implement proper input validation and sanitization to prevent XSS attacks.
    - Use HTTPS for all API calls and enforce secure headers.

2. **Vite Development Best Practices**:
    - Configure Vite to use secure plugins and avoid exposing sensitive information in the build process.
    - Ensure production builds are optimized and free of unnecessary debug information.
    - Use `vite-plugin-env-compatible` for secure environment variable management.
    - Enable strict CSP (Content Security Policy) headers in the Vite server configuration.

3. **General Secure Practices**:
    - Use modern authentication mechanisms (e.g., OAuth2, JWT) for user authentication.
    - Implement proper error handling to avoid leaking sensitive information.
    - Regularly update dependencies to patch known vulnerabilities.
    - Use tools like ESLint and Prettier to enforce secure coding standards.

Constraints:
- Ensure compatibility with Azure Static Web Apps deployment.
- Follow Azure best practices for secure web application development.
- Avoid using deprecated or insecure libraries.