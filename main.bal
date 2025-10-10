import ballerina/log;
import ballerinax/h2.driver as _;
import ballerinax/java.jdbc;

final jdbc:Client dbClient = check new (url = "jdbc:h2:file:./database/loandatabase", user = "test", password = "test");

public function main() returns error? {
    log:printInfo("[Main]: Starting ETL process");
    check initDB();

    // read the data from the csv file
    [Application[], ApprovedApplication[]] inputData = check extract();

    // perform transformations
    FinalReport report = check transform(inputData[0], inputData[1]);

    // Output the final data into database
    check load(report);

    log:printInfo("[Main]: Transformation completed successfully");
}

function extract() returns [Application[], ApprovedApplication[]]|error {
    log:printInfo("[Extract]: Starting data extraction from CSV files");

    string applicationsFile = "resources/request_applications.csv";
    string approvalsFile = "resources/approved_applications.csv";

    // TODO: Implement CSV reading logic
    // Hint: https://ballerina.io/learn/by-example/io-csv-datamapping/

    Application[] applications = [];
    ApprovedApplication[] approvals = [];

    log:printInfo("[Extract]: Completed data extraction from CSV files");
    return [applications, approvals];
}

function transform(Application[] applications, ApprovedApplication[] approvals) returns FinalReport|error {
    log:printInfo("[Transform]: Starting data transformations");

    // 1. JOIN - Enrich application data with approval information
    log:printInfo("[Transform - 1]: Start Joining applications with approvals");
    ApprovedApplicationDetail[] approvedApplications = enrichApplications(applications, approvals);
    log:printInfo("[Transform - 1]: Completed Joining applications with approvals");

    // 2. CALCULATION - Compute financial metrics
    log:printInfo("[Transform - 2]: Start Calculating financial metrics");
    LoanMetrics[] metrics = check calculateMetrics(approvedApplications);
    log:printInfo("[Transform - 2]: Completed Calculating financial metrics");

    // 3. CATEGORIZATION - Create categorical segments
    log:printInfo("[Transform - 3]: Start Categorizing applications");
    CategorizedApplication[] categorized = categorizeApplications(approvedApplications, metrics);
    log:printInfo("[Transform - 3]: Completed Categorizing applications");

    // 4. AGGREGATION - Branch Summary
    log:printInfo("[Transform - 4]: Start Branch Summary aggregation");
    BranchSummary[] branchSummary = aggregateBranchSummary(applications, approvals);
    log:printInfo("[Transform - 4]: Completed Branch Summary aggregation");

    // 5. AGGREGATION - Vehicle Summary
    log:printInfo("[Transform - 5]: Start Vehicle Summary aggregation");
    VehicleSummary[] vehicleSummary = aggregateVehicleSummary(applications);
    log:printInfo("[Transform - 5]: Completed Vehicle Summary aggregation");

    // 6. AGGREGATION - Officer Performance
    log:printInfo("[Transform - 6]: Start Officer Performance aggregation");
    OfficerPerformance[] officerPerf = aggregateOfficerPerformance(approvals);
    log:printInfo("[Transform - 6]: Completed Officer Performance aggregation");

    // 7. DATA QUALITY
    log:printInfo("[Transform - 7]: Start Data Quality validation");
    DataQualityFlag[] qualityFlags = validateDataQuality(applications, approvals);
    log:printInfo("[Transform - 7]: Completed Data Quality validation");

    FinalReport report = {
        branch_summaries: branchSummary,
        vehicle_summaries: vehicleSummary,
        officer_performances: officerPerf,
        data_quality_flags: qualityFlags,
        categorized_applications: categorized
    };

    return report;
}

function load(FinalReport report) returns error? {
    log:printInfo("[Load]: Starting data loading into the database");

    // Insert BranchSummary data
    check insertBranchSummaries(report.branch_summaries);
    log:printInfo("[Load]: Inserted BranchSummary data");

    // Insert VehicleSummary data
    check insertVehicleSummaries(report.vehicle_summaries);
    log:printInfo("[Load]: Inserted VehicleSummary data");

    // Insert OfficerPerformance data
    check insertOfficerPerformances(report.officer_performances);
    log:printInfo("[Load]: Inserted OfficerPerformance data");

    // Insert DataQualityFlag data
    check insertDataQualityFlags(report.data_quality_flags);
    log:printInfo("[Load]: Inserted DataQualityFlag data");

    // Insert CategorizedApplication data
    check insertCategorizedApplications(report.categorized_applications);
    log:printInfo("[Load]: Inserted CategorizedApplication data");

    log:printInfo("[Load]: Completed data loading into the database");
}

// TRANSFORMATION 1: Enrich application data with approval information
function enrichApplications(Application[] applications, ApprovedApplication[] approvals) returns ApprovedApplicationDetail[] {

    // TODO: Complete the join logic
    return from Application application in applications
        join ApprovedApplication approval in approvals on application.application_id equals approval.application_id
        select {
            application_id: application.application_id,
            approver: "",
            credit_score: 0,
            total_price: 0,
            city: "",
            vehicle_model: "",
            approved_on: "",
            monthly_income: 0,
            branch: "",
            requested_amount: 0,
            vehicle_brand: "",
            dob: "",
            fuel_type: "",
            approved_amount: 0,
            status: ""
        };
}

// TRANSFORMATION 2: CALCULATION - Compute financial metrics
function calculateMetrics(ApprovedApplicationDetail[] approvedApplications) returns LoanMetrics[]|error {
    LoanMetrics[] metrics = [];
    foreach var app in approvedApplications {
        // Calculate LTV (Loan-to-Value)
        decimal ltv = (app.requested_amount / app.total_price) * 100;

        // Simplified: assume 1 year term
        decimal monthlyPayment = app.requested_amount / 12; 
        decimal monthly_income = app.monthly_income;

        // Calculate DTI (Debt-to-Income) - monthly payment / monthly income
        decimal dti = (monthlyPayment / monthly_income) * 100;

        // Calculate age
        int age = check calculateAge(app.dob, app.approved_on);

        metrics.push({
            application_id: app.application_id,
            ltv_ratio: ltv,
            dti_ratio: dti,
            age_at_application: age
        });
    }
    return metrics;
}

// TRANSFORMATION 3: CATEGORIZATION - Create categorical segments
function categorizeApplications(ApprovedApplicationDetail[] apps, LoanMetrics[] metrics) returns CategorizedApplication[] {
    map<LoanMetrics> metricsMap = {};
    foreach var metric in metrics {
        metricsMap[metric.application_id] = metric;
    }

    CategorizedApplication[] categorized = [];
    foreach var app in apps {
        LoanMetrics metric = metricsMap.get(app.application_id);
        int age = metric.age_at_application;

        // Age grouping
        string ageGroup = categorizeAge(age);

        // Income bracketing
        string incomeBracket = categorizeIncome(app.monthly_income * 12); // Annualize income

        // Credit score banding
        string creditBand = categorizeCredit(app.credit_score);

        // Loan amount tiering
        string loanTier = categorizeLoan(app.requested_amount);

        // Risk category (composite scoring)
        string riskCategory = determineRiskCategory(app.credit_score, metric.dti_ratio, metric.ltv_ratio);

        categorized.push({
            application_id: app.application_id,
            age_group: ageGroup,
            income_bracket: incomeBracket,
            credit_score_band: creditBand,
            loan_amount_tier: loanTier,
            risk_category: riskCategory
        });
    }
    return categorized;
}

// TRANSFORMATION 4: AGGREGATION - Branch summary
function aggregateBranchSummary(Application[] apps, ApprovedApplication[] approvals) returns BranchSummary[] {
    map<int> totalApps = {};
    map<int> approvedCount = {};
    map<int> rejectedCount = {};
    map<decimal> totalApproved = {};

    // Count applications by branch
    foreach var app in apps {
        totalApps[app.branch] = (totalApps[app.branch] ?: 0) + 1;
        // TODO: Count approved and rejected applications
    }

    // Sum approved amounts
    map<string> appBranchMap = {};
    foreach var app in apps {
        appBranchMap[app.application_id] = app.branch;
    }

    foreach var approval in approvals {
        string? branch = appBranchMap[approval.application_id];
        if branch is string {
            totalApproved[branch] = (totalApproved[branch] ?: 0) + approval.approved_amount;
        }
    }

    BranchSummary[] summaries = [];
    foreach var [branch, total] in totalApps.entries() {
        int approved = approvedCount[branch] ?: 0;
        int rejected = rejectedCount[branch] ?: 0;
        decimal totalAmt = totalApproved[branch] ?: 0;

        summaries.push({
            branch: branch,
            total_applications: total,
            approved_count: approved,
            rejected_count: rejected,
            approval_rate: (approved * 100.0) / total,
            avg_approved_amount: approved > 0 ? totalAmt / approved : 0,
            total_approved_amount: totalAmt
        });
    }
    return summaries;
}

// TRANSFORMATION 5: AGGREGATION - Vehicle summary
function aggregateVehicleSummary(Application[] apps) returns VehicleSummary[] {
    map<record {int count; int approved;}> vehicleStats = {};

    foreach var app in apps {
        string key = app.vehicle_brand + "|" + app.fuel_type;
        var stats = vehicleStats[key];
        if stats is () {
            vehicleStats[key] = {count: 1, approved: app.status == "approved" ? 1 : 0};
        } else {
            stats.count += 1;
            stats.approved += app.status == "approved" ? 1 : 0;
        }
    }

    VehicleSummary[] summaries = [];
    // TODO: Fill vehicle summary data
    // Hint: Iterate over vehicleStats map and populate summaries array
    // use regex to split the key into brand and fuel_type
    // https://central.ballerina.io/ballerina/lang.map/2201.12.7#entries
    return summaries;
}

// TRANSFORMATION 6: AGGREGATION - Officer performance
function aggregateOfficerPerformance(ApprovedApplication[] approvals) returns OfficerPerformance[] {
    map<record {int count; decimal total;}> officerStats = {};

    foreach var approval in approvals {
        var stats = officerStats[approval.approver];
        if stats is () {
            officerStats[approval.approver] = {count: 1, total: approval.approved_amount};
        } else {
            stats.count += 1;
            stats.total += approval.approved_amount;
        }
    }

    OfficerPerformance[] performance = [];
    foreach var [officer, stats] in officerStats.entries() {
        performance.push({
            approver: officer,
            total_approvals: stats.count,
            total_amount_approved: stats.total,
            avg_approval_amount: stats.total / stats.count
        });
    }
    return performance;
}

// TRANSFORMATION 7: DATA QUALITY - Identify issues
function validateDataQuality(Application[] apps, ApprovedApplication[] approvals) returns DataQualityFlag[] {
    map<ApprovedApplication> approvalMap = {};
    foreach var approval in approvals {
        approvalMap[approval.application_id] = approval;
    }

    DataQualityFlag[] flags = [];

    foreach var app in apps {
        // Check 1: Approved status but no approval record
        if app.status == "approved" && approvalMap[app.application_id] is () {
            flags.push({
                application_id: app.application_id,
                issue_type: "MISSING_APPROVAL",
                description: "Application marked as approved but no approval record found"
            });
        }

        // Check 2: Approved amount exceeds total price
        ApprovedApplication? approval = approvalMap[app.application_id];
        if approval is ApprovedApplication && approval.approved_amount > app.total_price {
            flags.push({
                application_id: app.application_id,
                issue_type: "AMOUNT_ANOMALY",
                description: "Approved amount exceeds total vehicle price"
            });
        }

        // Check 3: High DTI ratio
        decimal monthlyPayment = app.amount / 12;
        decimal dti = (monthlyPayment / app.monthly_income) * 100;
        if dti > 50.0d {
            flags.push({
                application_id: app.application_id,
                issue_type: "HIGH_DTI",
                description: string `High debt-to-income ratio: ${dti}%`
            });
        }
    }

    return flags;
}

