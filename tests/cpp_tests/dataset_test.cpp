//
// Created by jorge on 3/14/22.
//

//#define BOOST_TEST_MODULE WT
#include <boost/test/unit_test.hpp>

#include <boost/format.hpp>

// /home/jorge/CLionProjects/mongo_ftdc/cmake-build-debug/tests/cpp_tests/Boost_Tests_run --run_test=WTSuite --logger=HRF,all --color_output=false --report_format=HRF --show_progress=no

struct TestFixtureDataset {

    TestFixtureDataset() {
        ;
    }

    ~TestFixtureDataset() {
        ;
    }

    void TestFixtureSetup() {
        BOOST_CHECK(true);
    }

};


//BOOST_FIXTURE_TEST_SUITE(WTSuite, TestFixtureDataset);

//BOOST_AUTO_TEST_SUITE(WTSuite)
BOOST_AUTO_TEST_SUITE(test_suite_name)


    int silly(int m) { return ++m; }

    BOOST_AUTO_TEST_CASE(test_wtCreate) {
        int m;
        m = silly(m);
        BOOST_CHECK_EQUAL(m, 0);
    }


    BOOST_AUTO_TEST_CASE(test_wtSillier) {
        int m;
        m = silly(m);
        BOOST_CHECK_EQUAL(m, m);
    }


BOOST_AUTO_TEST_SUITE_END();