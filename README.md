<!--
SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
SPDX-License-Identifier: Apache-2.0
-->

# Oblast

My attempt at an ORM library for Go. Inspired by [Gorp](https://pkg.go.dev/gopkg.in/gorp.v3), but without the bits that make Gorp slow.

## Unstructured notes

TODO: write out a proper readme

- goals: as fast as possible, as little allocs on hot paths as possible
- consequences: no `context.Context` (benchmarking shows up to 50% more allocations and 100% more memory allocated in OLTP usecase, i.e. QueryRow vs. QueryRowContext)
