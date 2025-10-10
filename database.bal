import ballerina/sql;

function initDB() returns error? {
    // Drop tables if they exist (in reverse order due to potential foreign key dependencies)
    sql:ParameterizedQuery drop1 = `DROP TABLE IF EXISTS CategorizedApplication`;
    sql:ParameterizedQuery drop2 = `DROP TABLE IF EXISTS DataQualityFlag`;
    sql:ParameterizedQuery drop3 = `DROP TABLE IF EXISTS OfficerPerformance`;
    sql:ParameterizedQuery drop4 = `DROP TABLE IF EXISTS VehicleSummary`;
    sql:ParameterizedQuery drop5 = `DROP TABLE IF EXISTS BranchSummary`;

    // Create BranchSummary table
    sql:ParameterizedQuery create1 = `CREATE TABLE BranchSummary (
        id INT NOT NULL AUTO_INCREMENT,
        branch VARCHAR(191) NOT NULL,
        total_applications INT NOT NULL,
        approved_count INT NOT NULL,
        rejected_count INT NOT NULL,
        approval_rate DECIMAL(5,2) NOT NULL,
        avg_approved_amount DECIMAL(15,2) NOT NULL,
        total_approved_amount DECIMAL(15,2) NOT NULL,
        PRIMARY KEY (id)
    )`;

    // Create VehicleSummary table
    sql:ParameterizedQuery create2 = `CREATE TABLE VehicleSummary (
        id INT NOT NULL AUTO_INCREMENT,
        vehicle_brand VARCHAR(191) NOT NULL,
        fuel_type VARCHAR(100) NOT NULL,
        application_count INT NOT NULL,
        approved_count INT NOT NULL,
        PRIMARY KEY (id)
    )`;

    // Create OfficerPerformance table
    sql:ParameterizedQuery create3 = `CREATE TABLE OfficerPerformance (
        id INT NOT NULL AUTO_INCREMENT,
        approver VARCHAR(191) NOT NULL,
        total_approvals INT NOT NULL,
        total_amount_approved DECIMAL(15,2) NOT NULL,
        avg_approval_amount DECIMAL(15,2) NOT NULL,
        PRIMARY KEY (id)
    )`;

    // Create DataQualityFlag table
    sql:ParameterizedQuery create4 = `CREATE TABLE DataQualityFlag (
        id INT NOT NULL AUTO_INCREMENT,
        application_id VARCHAR(191) NOT NULL,
        issue_type VARCHAR(191) NOT NULL,
        description TEXT NOT NULL,
        PRIMARY KEY (id)
    )`;

    // Create CategorizedApplication table
    sql:ParameterizedQuery create5 = `CREATE TABLE CategorizedApplication (
        id INT NOT NULL AUTO_INCREMENT,
        application_id VARCHAR(191) NOT NULL,
        age_group VARCHAR(100) NOT NULL,
        income_bracket VARCHAR(100) NOT NULL,
        credit_score_band VARCHAR(100) NOT NULL,
        loan_amount_tier VARCHAR(100) NOT NULL,
        risk_category VARCHAR(100) NOT NULL,
        PRIMARY KEY (id)
    )`;

    // Execute all queries
    _ = check dbClient->execute(drop1);
    _ = check dbClient->execute(drop2);
    _ = check dbClient->execute(drop3);
    _ = check dbClient->execute(drop4);
    _ = check dbClient->execute(drop5);
    _ = check dbClient->execute(create1);
    _ = check dbClient->execute(create2);
    _ = check dbClient->execute(create3);
    _ = check dbClient->execute(create4);
    _ = check dbClient->execute(create5);
}

// Insert function for BranchSummary data
function insertBranchSummaries(BranchSummary[] branchSummaries) returns error? {
    foreach BranchSummary summary in branchSummaries {
        sql:ParameterizedQuery insertQuery = `
            INSERT INTO BranchSummary (
                branch, total_applications, approved_count, rejected_count, 
                approval_rate, avg_approved_amount, total_approved_amount
            ) VALUES (
                ${summary.branch}, ${summary.total_applications}, ${summary.approved_count}, 
                ${summary.rejected_count}, ${summary.approval_rate}, ${summary.avg_approved_amount}, 
                ${summary.total_approved_amount}
            )
        `;
        _ = check dbClient->execute(insertQuery);
    }
}

// Insert function for VehicleSummary data
function insertVehicleSummaries(VehicleSummary[] vehicleSummaries) returns error? {
    foreach VehicleSummary summary in vehicleSummaries {
        sql:ParameterizedQuery insertQuery = `
            INSERT INTO VehicleSummary (
                vehicle_brand, fuel_type, application_count, approved_count
            ) VALUES (
                ${summary.vehicle_brand}, ${summary.fuel_type}, 
                ${summary.application_count}, ${summary.approved_count}
            )
        `;
        _ = check dbClient->execute(insertQuery);
    }
}

// Insert function for OfficerPerformance data
function insertOfficerPerformances(OfficerPerformance[] officerPerformances) returns error? {
    foreach OfficerPerformance performance in officerPerformances {
        sql:ParameterizedQuery insertQuery = `
            INSERT INTO OfficerPerformance (
                approver, total_approvals, total_amount_approved, avg_approval_amount
            ) VALUES (
                ${performance.approver}, ${performance.total_approvals}, 
                ${performance.total_amount_approved}, ${performance.avg_approval_amount}
            )
        `;
        _ = check dbClient->execute(insertQuery);
    }
}

// Insert function for DataQualityFlag data
function insertDataQualityFlags(DataQualityFlag[] dataQualityFlags) returns error? {
    foreach DataQualityFlag flag in dataQualityFlags {
        sql:ParameterizedQuery insertQuery = `
            INSERT INTO DataQualityFlag (
                application_id, issue_type, description
            ) VALUES (
                ${flag.application_id}, ${flag.issue_type}, ${flag.description}
            )
        `;
        _ = check dbClient->execute(insertQuery);
    }
}

// Insert function for CategorizedApplication data
function insertCategorizedApplications(CategorizedApplication[] categorizedApplications) returns error? {
    foreach CategorizedApplication app in categorizedApplications {
        sql:ParameterizedQuery insertQuery = `
            INSERT INTO CategorizedApplication (
                application_id, age_group, income_bracket, credit_score_band, 
                loan_amount_tier, risk_category
            ) VALUES (
                ${app.application_id}, ${app.age_group}, ${app.income_bracket}, 
                ${app.credit_score_band}, ${app.loan_amount_tier}, ${app.risk_category}
            )
        `;
        _ = check dbClient->execute(insertQuery);
    }
}
