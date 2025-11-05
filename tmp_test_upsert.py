from src.db import upsert_verification_result

# 1) First write
upsert_verification_result(
    email='alice@example.com',
    verify_status='valid',
    reason='seed-1',
    mx_host='mx1.example.com',
)

# 2) Second write (same email, should UPDATE not INSERT)
upsert_verification_result(
    email='alice@example.com',
    verify_status='invalid',
    reason='seed-2',
    mx_host='mx2.example.com',
)

print('upsert calls ok')
