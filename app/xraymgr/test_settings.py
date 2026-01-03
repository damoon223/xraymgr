# تنظیمات فاز تست (فعلاً دستی؛ بعداً از طریق پنل)

# pool تست: 100 پورت => 9000..9099
TEST_PORT_START = 9000
TEST_PORT_END = 9099
TEST_POOL_SIZE = 100

# اجرای تست‌ها هم‌زمان
TEST_CONCURRENCY = 10

# timeout/lease برای قفل تست (ثانیه)
TEST_LOCK_TIMEOUT_SECONDS = 60

# استایل tag برای inbound تست: in_test_<port>
TEST_INBOUND_TAG_PREFIX = "in_test_"

# مشخصات موقت inbound تست (SOCKS5)
TEST_SOCKS_USER = "me"
TEST_SOCKS_PASS = "1"
