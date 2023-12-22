find_path(CUNIT_INCLUDE_DIRS CUnit/CUnit.h)
find_library(CUNIT_LIBRARIES NAMES CUnit cunit)

include(FindPackageHandleStandardArgs)

find_package_handle_standard_args(CUnit
        REQUIRED_VARS CUNIT_INCLUDE_DIRS CUNIT_LIBRARIES
        VERSION_VAR CUNIT_VERSION)
