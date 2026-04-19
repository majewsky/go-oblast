<!--
SPDX-FileCopyrightText: 2026 Stefan Majewsky <majewsky@gmx.net>
SPDX-License-Identifier: Apache-2.0
-->

- TODO: consider adding an upsert, e.g. `func (Store[R]) InsertOrUpdate(db Handle, records ...*R) error`, that chooses based on whether any auto fields is non-zero
