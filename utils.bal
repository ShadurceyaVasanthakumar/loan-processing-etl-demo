import ballerina/time;

function calculateAge(string dobStr, string createdAtStr) returns int|error {
    time:Date|error dob = check parseTime(dobStr);
    time:Date|error createdAt = check parseTime(createdAtStr);
    if (dob is time:Date && createdAt is time:Date) {
        return createdAt.year - dob.year;
    }
    return error("Invalid date format");
}

function parseTime(string dateStr) returns time:Date|error {
    string[] split = re `-`.split(dateStr);
    return {year: check int:fromString(split[0]), month: check int:fromString(split[1]), day: check int:fromString(split[2])};
}

function categorizeAge(int age) returns string {
    if age < 30 {
        return Young_Age;
    } else if age <= 50 {
        return Mid_Age;
    } 
    return Senior_Age;
}

function categorizeIncome(decimal income) returns string {
    // TODO: Implement income bracketing logic
    // Income bracketing
    //  if income < 200K: Low
    //  if 200K <= income <= 300K: Medium
    //  if income > 300K: High
    return High_Income;
}


function categorizeCredit(int score) returns string {
    // TODO: Implement credit score banding logic
    // Credit score banding
    // if score < 650: Poor
    // if 650 <= score < 700: Fair
    // if 700 <= score < 750: Good
    // if score >= 750: Excellent
    return Excellent_Credit;
}


function categorizeLoan(decimal amount) returns string {
    // TODO: Implement loan amount tiering logic
    // Loan amount tiering
    // if amount < 5M: Small
    // if 5M <= amount <= 10M: Medium
    // if amount > 10M: Large
    return Large_Loan;
}

function determineRiskCategory(int creditScore, decimal dti, decimal ltv) returns string {
    if creditScore >= 750 && dti < 30.0d && ltv < 70.0d {
        return Low_Risk;
    } else if creditScore >= 700 && dti < 40.0d && ltv < 80.0d {
        return Medium_Risk;
    }
    return High_Risk;
}
