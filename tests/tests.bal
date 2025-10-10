import ballerina/io;
import ballerina/sql;
import ballerina/test;
import ballerinax/h2.driver as _;

type Count record {|
    int count;
|};

@test:BeforeSuite
function runETL() returns error? {
    check main();
}

@test:Config
function testBranchSummary() returns error? {
    stream<Count, sql:Error?> branchSummaryCountStrm = dbClient->query(
        `SELECT COUNT(*) as count FROM BranchSummary`);
    record {|Count value;|}? actualBranchSummaryCount = check branchSummaryCountStrm.next();
    if actualBranchSummaryCount !is () {
        test:assertTrue(actualBranchSummaryCount.value.count >= 0,
                "BranchSummary table should exist and have data");
    }
}

@test:Config
function testVehicleSummary() returns error? {
    stream<Count, sql:Error?> vehicleSummaryCountStrm = dbClient->query(
        `SELECT COUNT(*) as count FROM VehicleSummary`);
    record {|Count value;|}? actualVehicleSummaryCount = check vehicleSummaryCountStrm.next();
    if actualVehicleSummaryCount !is () {
        test:assertTrue(actualVehicleSummaryCount.value.count >= 0,
                "VehicleSummary table should exist and have data");
    }
}

@test:Config
function testOfficerPerformance() returns error? {
    stream<Count, sql:Error?> officerPerfCountStrm = dbClient->query(
        `SELECT COUNT(*) as count FROM OfficerPerformance`);
    record {|Count value;|}? actualOfficerPerfCount = check officerPerfCountStrm.next();
    if actualOfficerPerfCount !is () {
        test:assertTrue(actualOfficerPerfCount.value.count >= 0,
                "OfficerPerformance table should exist and have data");
    }
}

@test:Config
function testDataQualityFlag() returns error? {
    stream<Count, sql:Error?> dataQualityCountStrm = dbClient->query(
        `SELECT COUNT(*) as count FROM DataQualityFlag`);
    record {|Count value;|}? actualDataQualityCount = check dataQualityCountStrm.next();
    if actualDataQualityCount !is () {
        test:assertTrue(actualDataQualityCount.value.count >= 0,
                "DataQualityFlag table should exist and have data");
    }
}

@test:Config
function testCategorizedApplication() returns error? {
    stream<Count, sql:Error?> categorizedAppCountStrm = dbClient->query(
        `SELECT COUNT(*) as count FROM CategorizedApplication`);
    record {|Count value;|}? actualCategorizedAppCount = check categorizedAppCountStrm.next();
    if actualCategorizedAppCount !is () {
        test:assertTrue(actualCategorizedAppCount.value.count >= 0,
                "CategorizedApplication table should exist and have data");
    }
}

@test:Config
function assertBranchSummary() returns error? {
    // read the data from the csv file
    [Application[], ApprovedApplication[]] inputData = check extract();

    json readJson = check io:fileReadJson("tests/final_report.json");
    FinalReport finalReport = check readJson.cloneWithType();

    BranchSummary[] branchSummaries = aggregateBranchSummary(inputData[0], inputData[1]);
    test:assertEquals(branchSummaries, finalReport.branch_summaries, msg = "BranchSummary data mismatch");

}

@test:Config
function assertVehicleSummary() returns error? {
    // read the data from the csv file
    [Application[], ApprovedApplication[]] inputData = check extract();

    json readJson = check io:fileReadJson("tests/final_report.json");
    FinalReport finalReport = check readJson.cloneWithType();

    VehicleSummary[] vehicleSummaries = aggregateVehicleSummary(inputData[0]);
    test:assertEquals(vehicleSummaries, finalReport.vehicle_summaries, msg = "VehicleSummary data mismatch");

}

@test:Config
function assertOfficerPerformance() returns error? {
    // read the data from the csv file
    [Application[], ApprovedApplication[]] inputData = check extract();

    json readJson = check io:fileReadJson("tests/final_report.json");
    FinalReport finalReport = check readJson.cloneWithType();

    OfficerPerformance[] officerPerformances = aggregateOfficerPerformance(inputData[1]);
    test:assertEquals(officerPerformances, finalReport.officer_performances, msg = "OfficerPerformance data mismatch");
}

@test:Config
function assertDataQualityFlag() returns error? {
    // read the data from the csv file
    [Application[], ApprovedApplication[]] inputData = check extract();

    json readJson = check io:fileReadJson("tests/final_report.json");
    FinalReport finalReport = check readJson.cloneWithType();

    DataQualityFlag[] dataQualityFlags = validateDataQuality(inputData[0], inputData[1]);
    test:assertEquals(dataQualityFlags, finalReport.data_quality_flags, msg = "DataQualityFlag data mismatch");
}

function assertCategorizedApplication() returns error? {
    // read the data from the csv file
    [Application[], ApprovedApplication[]] inputData = check extract();
    ApprovedApplicationDetail[] approvedApplications = enrichApplications(inputData[0], inputData[1]);

    json readJson = check io:fileReadJson("tests/final_report.json");
    FinalReport finalReport = check readJson.cloneWithType();

    LoanMetrics[] metrics = check calculateMetrics(approvedApplications);
    CategorizedApplication[] categorizedApplications = categorizeApplications(approvedApplications, metrics);
    test:assertEquals(categorizedApplications, finalReport.categorized_applications, msg = "CategorizedApplication data mismatch");
}
