add_executable(test_core test_core.cpp)
target_link_libraries(test_core PRIVATE FailoverFabric)
add_test(NAME test_core COMMAND test_core)

add_executable(test_scenarios test_scenarios.cpp)
target_link_libraries(test_scenarios PRIVATE FailoverFabric)
add_test(NAME test_scenarios COMMAND test_scenarios)

add_executable(test_property test_property.cpp)
target_link_libraries(test_property PRIVATE FailoverFabric)
add_test(NAME test_property COMMAND test_property)

add_executable(test_concurrency test_concurrency.cpp)
target_link_libraries(test_concurrency PRIVATE FailoverFabric)
add_test(NAME test_concurrency COMMAND test_concurrency)

add_executable(test_adversarial test_adversarial.cpp)
target_link_libraries(test_adversarial PRIVATE FailoverFabric)
add_test(NAME test_adversarial COMMAND test_adversarial)
