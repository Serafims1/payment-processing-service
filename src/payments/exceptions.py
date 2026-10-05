class ApplicationError(Exception):
    pass


class PaymentNotFound(ApplicationError):
    pass


class IdempotencyConflict(ApplicationError):
    pass


class DeliveryExhausted(ApplicationError):
    pass
