# Contributions are welcome

The project adheres to [Semantic Versioning](https://semver.org)
and [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
This repo uses Docker-backed `make` targets for its lint, test, mock, and
protobuf-generation workflows. The only host-side prerequisites are
[Docker](https://docs.docker.com/get-docker/) and `make`.

There are two ways you can become a contributor:

1. Request to become a collaborator, and then you can just open pull requests against the repository without forking it.
2. Follow these steps
    - Fork the repository
    - Create a feature branch
    - Set your docker credentials on your fork using the following secret names: `DOCKER_USER` and `DOCKER_PASS`
    - Submit a [pull request](https://help.github.com/articles/using-pull-requests)

## Test & Linter

Prior to submitting a [pull request](https://help.github.com/articles/using-pull-requests),
please build the CI tooling image once and then run lint and tests:

```bash
make docker-image   # builds ego-ci:latest from Dockerfile.ci (one-time per change)
make docker-ci      # runs docker-lint + docker-test
```

Each target also works on its own:

| Target            | Purpose                                                              |
|-------------------|----------------------------------------------------------------------|
| `docker-image`    | Build the hermetic CI image (`ego-ci:latest`) from `Dockerfile.ci`.  |
| `docker-lint`     | Run `golangci-lint` against the working tree.                        |
| `docker-test`     | Run the test suite with the race detector and coverage.              |
| `docker-mock`     | Regenerate the mocks under `./mocks` via `mockery`.                  |
| `docker-protogen` | Regenerate protobuf code via `buf` and refresh `example/examplepb`.  |
| `docker-ci`       | Composite of `docker-lint` + `docker-test`.                          |

The Docker targets mount the working tree at `/workspace` inside the container
and run as your local UID/GID, so any generated files (mocks, protobufs,
`coverage.out`) appear in the repo with normal ownership. Go build and module
caches are kept warm between runs in the named volumes `ego-go-build-cache`
and `ego-go-mod-cache`.

If you have the toolchain installed locally, the convenience targets
`make run-eventsourced`, `make run-durablestate`, `make run-saga`, and
`make proto` are still available and execute directly on the host without
Docker.
