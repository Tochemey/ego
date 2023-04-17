# Ego

[![build](https://img.shields.io/github/actions/workflow/status/Tochemey/ego/build.yml?branch=main)](https://github.com/Tochemey/ego/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/Tochemey/ego/branch/main/graph/badge.svg?token=Z5b9gM6Mnt)](https://codecov.io/gh/Tochemey/ego)

Ego is a minimal library that help build event-sourcing and CQRS application through a simple interface, and it allows developers to describe their schema with Protobuf.

## Features

- [x] Write Model:
    - [x] Commands handler
    - [x] Events handler
- [ ] Read Model

## Contribution
Contributions are welcome!
The project adheres to [Semantic Versioning](https://semver.org) and [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
This repo uses [Earthly](https://earthly.dev/get-earthly).

To contribute please:
- Fork the repository
- Create a feature branch
- Submit a [pull request](https://help.github.com/articles/using-pull-requests)

### Test & Linter
Prior to submitting a [pull request](https://help.github.com/articles/using-pull-requests), please run:
```bash
earthly +test
```
