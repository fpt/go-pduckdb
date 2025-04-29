# Contributing to go-pduckdb

Thank you for considering contributing to go-pduckdb! This document outlines the process and guidelines for contributing to this project.

## Code of Conduct

By participating in this project, you agree to maintain a respectful and inclusive environment for everyone. Please report any unacceptable behavior to the project maintainers.

## Contributions

There are many ways to contribute to go-pduckdb:

- Reporting bugs or issues
- Suggesting enhancements or new features
- Improving documentation
- Writing or improving tests
- Submitting code changes or fixes
- Reviewing pull requests

### Reporting Bugs

When reporting bugs, please include:

- A clear description of the issue
- Steps to reproduce the behavior
- Expected vs. actual results
- Version information (Go version, DuckDB version, OS)
- Any relevant logs or error messages

Please use the GitHub issue tracker to report bugs.

### Feature Requests

For feature requests, please provide:

- A clear description of the feature
- The motivation or use case for the feature
- Any relevant examples or references

## Development Process

1. Fork the repository
2. Create a new branch from `main`
3. Make your changes
4. Add or update tests as necessary
5. Ensure all tests pass by running `make test`
6. Commit your changes with clear commit messages
7. Push your branch to your forked repository
8. Submit a pull request

## Pull Request Guidelines

When submitting a pull request:

- Provide a clear description of the changes made
- Reference any related issues
- Ensure all tests pass
- Include new tests for new functionality
- Update documentation as needed
- Follow the coding standards and style guide

Pull requests should be focused on a single issue or feature to simplify the review process.

## Golang Guidelines

### Code Style

- Follow [Go's official style guide](https://golang.org/doc/effective_go)
- Use `gofmt` to format your code
- Follow the principles in [Effective Go](https://golang.org/doc/effective_go)
- Use meaningful and descriptive variable/function names
- Keep functions small and focused on a single responsibility

### Testing

- Write unit tests for all new functionality
- Aim for high test coverage, especially for critical paths
- Use table-driven tests where appropriate
- Test edge cases and error conditions

### Documentation

- Document all exported functions, types, and constants
- Include examples where appropriate
- Keep documentation up to date when making changes

### Dependency Management

- Minimize external dependencies when possible
- For necessary dependencies, use Go modules for version management
- Document any new dependencies in the README.md

## License

By contributing to go-pduckdb, you agree that your contributions will be licensed under the same license as the project.

## Questions?

If you have any questions about contributing, please open an issue or reach out to the maintainers.

Thank you for your contributions to go-pduckdb!

