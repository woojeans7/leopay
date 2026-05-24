rootProject.name = "payment-service"

include(
    "core:core-enum",
    "storage:db-core",
    "support:logging",
    "support:monitoring",
    "booking-api",
    "mock-pg",
    "notification-worker",
    "settlement-worker",
)
