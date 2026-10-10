# Fault report

Source: `benchmarking/output/faults/influxdb` — 2 fault runs.

Each run holds a dependency down for a fixed outage and watches what the pipeline does. Every column is derived from the subscriber's raw cumulative counters, not from a rate panel, because a 15s rate window smears the abrupt stop these runs are about.

A sample where the subscriber served no metrics at all counts as stopped: from the pipeline's point of view a subscriber that ingests nothing and a subscriber that is not there are the same outcome. `dark_s` is how much of the run was in that state.

`never` in a resume column means the pipeline had not recovered by the end of the run. That is a result, not missing data. `n/a` means that counter never stopped, so there was nothing to resume. The `restarts` column is Docker's own restart counter, which an explicit stop and start does not increment, so it shows blast radius and never the fault itself.

| scenario | target | outage_s | rep | sub_restarted | restarts | dark_s | ingest_stop_s | ingest_resume_s | commit_stop_s | commit_resume_s | final_gap |
|---|---|---|---|---|---|---|---|---|---|---|---|
| load_p20.env | broker | 30 | 1 | yes | subscriber x8 | 36 | 2 | never | 2 | never | 0 |
| load_p20.env | db | 30 | 1 | no | none | 0 | never | n/a | 5 | 4 | 607,167 |

`final_gap` is ingested minus committed at the last sample. A gap that stays open is a subscriber that kept accepting messages it can no longer write. Where the subscriber restarted, both counters restarted with it, so the gap covers only the time since.

The `sensor_status` blast radius is not derived here, because reading it needs a client for whichever database is loaded. `sub_restarted` already carries the signal it stood for: a restart loses every per-sensor actor's in-memory state.

## load_p20.env — broker, 30s outage (rep 1)

- `61s` — `docker compose stop mosquitto`
- `90s` — `docker compose start mosquitto`

| phase | samples | unreachable | ingested | committed | ingest/s | commit/s |
|---|---|---|---|---|---|---|
| baseline | 29 | 0 | 1,159,951 | 1,160,350 | 19,999 | 20,006 |
| outage | 14 | 11 | 0 | 0 | 0 | 0 |
| recovery | 38 | 7 | 0 | 0 | 0 | 0 |

## load_p20.env — db, 30s outage (rep 1)

- `61s` — `docker compose stop influxdb`
- `90s` — `docker compose start influxdb`

| phase | samples | unreachable | ingested | committed | ingest/s | commit/s |
|---|---|---|---|---|---|---|
| baseline | 29 | 0 | 1,157,270 | 1,158,000 | 19,953 | 19,966 |
| outage | 14 | 0 | 519,972 | 33,050 | 18,570 | 1,180 |
| recovery | 38 | 0 | 1,519,976 | 1,440,000 | 20,000 | 18,947 |

## Provenance

| key | value |
|---|---|
| runs | 2 |
| sample interval | 2s |
| fault injected at | 61 s elapsed |
| outage lengths | 30 s |
| timing resolution | one sample interval plus one 2s Prometheus scrape |

Timings are recorded when the stop or start command was issued, which is not quite when the dependency became unreachable: `docker compose stop` sends SIGTERM and waits out the container's grace period. Comparing two runtimes against the same command is the point; reading an absolute number off one of them is not.
