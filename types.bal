type Application record {|
    string application_id;
    string dob;
    string city;
    decimal amount;
    decimal total_price;
    decimal monthly_income;
    int credit_score;
    string branch;
    string importer_category;
    string importer_name;
    string vehicle_brand;
    string vehicle_model;
    string vehicle_category;
    string fuel_type;
    string created_at;
    string status;
|};

type ApprovedApplication record {|
    string application_id;
    decimal approved_amount;
    string approved_on;
    string approver;
|};

type ApprovedApplicationDetail record {|
    string application_id;
    string dob;
    string status;
    string city;
    string branch;
    decimal requested_amount;
    decimal approved_amount;
    decimal total_price;
    string approver;
    string approved_on;
    string vehicle_brand;
    string vehicle_model;
    string fuel_type;
    int credit_score;
    decimal monthly_income;
|};

type LoanMetrics record {|
    string application_id;
    decimal ltv_ratio; // Loan-to-Value
    decimal dti_ratio; // Debt-to-Income
    int age_at_application;
|};

type CategorizedApplication record {|
    string application_id;
    string age_group;
    string income_bracket;
    string credit_score_band;
    string loan_amount_tier;
    string risk_category;
|};

type BranchSummary record {|
    string branch;
    int total_applications;
    int approved_count;
    int rejected_count;
    decimal approval_rate;
    decimal avg_approved_amount;
    decimal total_approved_amount;
|};

type VehicleSummary record {|
    string vehicle_brand;
    string fuel_type;
    int application_count;
    int approved_count;
|};

type OfficerPerformance record {|
    string approver;
    int total_approvals;
    decimal total_amount_approved;
    decimal avg_approval_amount;
|};

type DataQualityFlag record {|
    string application_id;
    string issue_type;
    string description;
|};

type FinalReport record {|
    BranchSummary[] branch_summaries;
    VehicleSummary[] vehicle_summaries;
    OfficerPerformance[] officer_performances;
    DataQualityFlag[] data_quality_flags;
    CategorizedApplication[] categorized_applications;
|};

enum AgeGroup {
    Young_Age = "Young (18-30)",
    Mid_Age = "Mid (31-50)",
    Senior_Age = "Senior (51+)"
}

enum IncomeBracket {
    Low_Income = "Low (<200K)",
    Medium_Income = "Medium (200K-300K)",
    High_Income = "High (>300K)"
}

enum CreditScoreBand {
    Poor_Credit = "Poor (<650)",
    Fair_Credit = "Fair (650-699)",
    Good_Credit = "Good (700-749)",
    Excellent_Credit = "Excellent (750+)"
}

enum LoanAmountTier {
    Small_Loan = "Small (<5M)",
    Medium_Loan = "Medium (5M-10M)",
    Large_Loan = "Large (>10M)"
}

enum RiskCategory {
    Low_Risk = "Low Risk",
    Medium_Risk = "Medium Risk",
    High_Risk = "High Risk"
}
