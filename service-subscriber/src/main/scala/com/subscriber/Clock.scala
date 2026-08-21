package com.subscriber

import java.time.Instant

// The one microsecond stamp definition for the subscriber; reading a latency clock as millis anywhere
// re-quantises the measurement. Erlang gets the same from os:system_time(microsecond) directly.
object Clock {
  // Microseconds since the Unix epoch for an already-captured instant
  def micros(i: Instant): Long = i.getEpochSecond * 1000000L + i.getNano / 1000

  // Microseconds since the Unix epoch, read now
  def nowMicros(): Long = micros(Instant.now())
}
