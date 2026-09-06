package com.subscriber

import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}

// The one microsecond stamp definition for the subscriber; reading a latency clock as millis anywhere
// re-quantises the measurement. Erlang gets the same from os:system_time(microsecond) directly.
object Clock {
  // Microseconds since the Unix epoch for an already-captured instant
  def micros(i: Instant): Long = i.getEpochSecond * 1000000L + i.getNano / 1000

  // Microseconds since the Unix epoch, read now
  def nowMicros(): Long = micros(Instant.now())

  // The inverse, so the epoch value stays the only representation crossing DatabaseBackend --
  // mirrors micros_to_datetime/1. floorDiv/floorMod: /,% would round pre-epoch into the wrong second.
  def toOffsetDateTime(micros: Long): OffsetDateTime =
    OffsetDateTime.ofInstant(
      Instant.ofEpochSecond(Math.floorDiv(micros, 1000000L), Math.floorMod(micros, 1000000L) * 1000L),
      ZoneOffset.UTC)

  // The zoneless form, for a column type that stores no offset (MySQL DATETIME(6)). Bound as UTC to
  // match what toOffsetDateTime writes to timestamptz, so a reading lands at the same instant in
  // either database and the read group's bounded window means the same thing in both. Both
  // conversions live here rather than in a backend so no backend can pick a different zone.
  def toLocalDateTimeUtc(micros: Long): LocalDateTime =
    toOffsetDateTime(micros).toLocalDateTime
}
