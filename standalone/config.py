# Shared configuration for standalone data fetchers

# Number of days to keep in the rolling CSV backlog
DAYS_BACKLOG = 14

# Maximum age (in hours) for data to be considered "fresh" (skip fetch if newer)
FRESHNESS_HOURS = 12

# Minimum hours between fetches (guard: fetch at most once per this interval)
DAILY_GUARD_HOURS = 20
