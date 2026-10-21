# Changelog

## 0.4.0 (2026-10-21)

### Features

- **postgres**: pgkit for migrations and LISTEN, a ledger prefix read, byte-ordered keys (#5) ([078fd82](https://github.com/urmzd/anchor/commit/078fd82554ea493779749438885bb7a543aea442))

[Full Changelog](https://github.com/urmzd/anchor/compare/v0.3.0...v0.4.0)


## 0.3.0 (2026-10-18)

### Features

- **pgqueue**: Postgres DeltaLog for streaming without Redis (#4) ([3213600](https://github.com/urmzd/anchor/commit/3213600d071a947b389402420e274e86cfd9d7ea))

[Full Changelog](https://github.com/urmzd/anchor/compare/v0.2.0...v0.3.0)


## 0.2.0 (2026-10-18)

### .

#### Misc

- **release**: read the sr-releaser key from the release environment (#2) ([97864b2](https://github.com/urmzd/anchor/commit/97864b205e0cc03fc8221938bca0097889034dd6))
- upgrade urmzd/sr action to v9 (#1) ([f3031fb](https://github.com/urmzd/anchor/commit/f3031fb9d46d9c2fc3d0954fd80a48361c0384a1))

### adapters/postgres

#### Breaking

- rename duraturo to anchor (#3) ([aaaad82](https://github.com/urmzd/anchor/commit/aaaad82fbda62b81616f840dff7c01e54cf88dd3))

### adapters/redis

#### Breaking

- rename duraturo to anchor (#3) ([aaaad82](https://github.com/urmzd/anchor/commit/aaaad82fbda62b81616f840dff7c01e54cf88dd3))

[Full Changelog](https://github.com/urmzd/anchor/compare/v0.1.1...v0.2.0)


## 0.1.1 (2026-07-15)

### .

#### Bug Fixes

- **ci**: tidy example modules against the v0.1.0 adapter pins ([b2f56a9](https://github.com/urmzd/duraturo/commit/b2f56a965f49a6f106088314a386047951431b0f))

### adapters/postgres

#### Misc

- pin adapters to duraturo v0.1.0 ([45e72b0](https://github.com/urmzd/duraturo/commit/45e72b002e4ceb2b186d888e0dabb9e36f818df7))

### adapters/redis

#### Misc

- pin adapters to duraturo v0.1.0 ([45e72b0](https://github.com/urmzd/duraturo/commit/45e72b002e4ceb2b186d888e0dabb9e36f818df7))

[Full Changelog](https://github.com/urmzd/duraturo/compare/v0.1.0...v0.1.1)


## 0.1.0 (2026-07-15)

### adapters/postgres

#### Features

- duraturo, durable execution for existing Go code ([c8f352a](https://github.com/urmzd/duraturo/commit/c8f352a0f22f807824951a50b0780eb695746680))

### adapters/redis

#### Features

- duraturo, durable execution for existing Go code ([c8f352a](https://github.com/urmzd/duraturo/commit/c8f352a0f22f807824951a50b0780eb695746680))
