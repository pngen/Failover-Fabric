// framework.hpp — tiny deterministic test harness. No timeouts of any kind.
#pragma once
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <functional>
#include <sstream>
#include <string>
#include <vector>

namespace ff_test {

struct Failure { std::string message; };

#define FF_CHECK(cond) do { if (!(cond)) {     std::ostringstream oss; oss << __FILE__ << ":" << __LINE__ << " CHECK failed: " #cond << "   ";     throw ff_test::Failure{oss.str()}; } } while (0)
#define FF_CHECK_EQ(a, b) do { auto _a = (a); auto _b = (b); if (!(_a == _b)) {     std::ostringstream oss; oss << __FILE__ << ":" << __LINE__ << " CHECK_EQ failed: " #a " == " #b;     throw ff_test::Failure{oss.str()}; } } while (0)
#define FF_REQUIRE_THROW(expr, Ex) do { bool _threw = false; try { (void)(expr); } catch (const Ex&) { _threw = true; }     if (!_threw) { std::ostringstream oss; oss << __FILE__ << ":" << __LINE__ << " expected throw " #Ex;     throw ff_test::Failure{oss.str()}; } } while (0)

struct TestCase { const char* name; std::function<void()> fn; };
inline std::vector<TestCase>& registry() { static std::vector<TestCase> r; return r; }
struct Registrar { Registrar(const char* n, std::function<void()> f) { registry().push_back({n, f}); } };
#define FF_TEST(name)   static void ff_test_##name();   static ff_test::Registrar ff_reg_##name(#name, ff_test_##name);   static void ff_test_##name()

inline int run_all() {
  int failed = 0;
  for (const TestCase& t : registry()) {
    try { t.fn(); std::fprintf(stderr, "[PASS] %s\n", t.name); }
    catch (const Failure& f) { ++failed; std::fprintf(stderr, "[FAIL] %s : %s\n", t.name, f.message.c_str()); }
    catch (const std::exception& e) { ++failed; std::fprintf(stderr, "[FAIL] %s : std::exception %s\n", t.name, e.what()); }
    catch (...) { ++failed; std::fprintf(stderr, "[FAIL] %s : unknown exception\n", t.name); }
  }
  std::fprintf(stderr, "=== %d checks, %d failures ===\n", (int)registry().size(), failed);
  return failed == 0 ? 0 : 1;
}

}  // namespace ff_test