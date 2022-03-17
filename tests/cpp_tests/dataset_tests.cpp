//
// Created by jorge on 3/14/22.
//
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <boost/test/unit_test.hpp>

#include <boost/format.hpp>

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

BOOST_FIXTURE_TEST_SUITE(DatasetSuite, TestFixtureDataset);

BOOST_AUTO_TEST_SUITE(DatasetSuite)

    BOOST_AUTO_TEST_CASE(test_Dataset) {
        BOOST_CHECK_EQUAL(0, 0);
    }

    BOOST_AUTO_TEST_CASE(test_Create) {
        int m;
        m = 0;
        BOOST_CHECK_EQUAL(m, 0);
    }
}

BOOST_AUTO_TEST_SUITE_END();
