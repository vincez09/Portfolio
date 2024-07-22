-- Delete duplicates
WITH duplicates AS (
SELECT
	ctid,
	ROW_NUMBER() OVER(PARTITION BY
		loan_amnt,
		int_rate,
		installment,
		grade,
		sub_grade,
		verification_status,
		issue_d,
		loan_status,
		purpose,
		title,
		dti,
		initial_list_status,
		application_type,
		emp_title,
		emp_length,
		home_ownership,
		zip_code,
		addr_state,
		earliest_cr_line,
		annual_inc,
		open_acc,
		pub_rec,
		revol_bal,
		revol_util,
		total_acc,
		mort_acc,
		pub_rec_bankruptcies
	ORDER BY NULL) AS row_number
FROM accepted_loans)

DELETE FROM accepted_loans
WHERE ctid IN (
	SELECT ctid
	FROM duplicates
	WHERE row_number > 1);
	
-- Drop id column then insert loan_id column
ALTER TABLE accepted_loans
DROP COLUMN id;

ALTER TABLE accepted_loans
ADD COLUMN loan_id SERIAL;

-- Insert borrower_id column
ALTER TABLE accepted_loans
ADD COLUMN borrower_id SERIAL;

-- Create loans table
DROP TABLE IF EXISTS loans;
CREATE TABLE loans (
	"loan_id" SERIAL  NOT NULL,
	"borrower_id" BIGINT,
	"loan_amnt" DOUBLE PRECISION,
	"int_rate" DOUBLE PRECISION,
	"installment" DOUBLE PRECISION,
	"grade" TEXT,
	"sub_grade" TEXT,
	"verification_status" TEXT,
	"issue_d" DATE,
	"loan_status" TEXT,
	"purpose" TEXT,
	"title" TEXT,
	"initial_list_status" TEXT,
	"application_type" TEXT,
	PRIMARY KEY ("loan_id")
);

--Create borrower table
DROP TABLE IF EXISTS borrower;
CREATE TABLE borrower (
	"borrower_id" SERIAL,
	"emp_title" TEXT,
	"emp_length" TEXT,
	"home_ownership" TEXT,
	"zip_code" TEXT,
	"addr_state" TEXT,
	"earliest_cr_line" DATE,
	"annual_inc" DOUBLE PRECISION,
	"open_acc" INTEGER,
	"pub_rec" INTEGER,
	"dti" DOUBLE PRECISION,
	"revol_bal" DOUBLE PRECISION,
	"revol_util" DOUBLE PRECISION,
	"total_acc" INTEGER,
	"mort_acc" INTEGER,
	"pub_rec_bankruptcies" INTEGER,
	PRIMARY KEY ("borrower_id")
);

-- Insert data into loans table from staging table
INSERT INTO loans (loan_id, borrower_id, loan_amnt, int_rate, installment, grade, sub_grade, verification_status, 
				   issue_d, loan_status, purpose, title, initial_list_status, application_type)
	SELECT 
	CAST(loan_id AS INT), borrower_id, loan_amnt, int_rate, installment, grade, sub_grade, verification_status, 
	TO_DATE(issue_d, 'Mon-YYYY'), loan_status, purpose, title, initial_list_status, application_type
	FROM accepted_loans

-- Insert data into borrower table from staging table
INSERT INTO borrower (borrower_id, emp_title, emp_length, home_ownership, zip_code, addr_state, earliest_cr_line, 
					  annual_inc, open_acc, pub_rec, dti, revol_bal, revol_util, total_acc, mort_acc, pub_rec_bankruptcies)
	SELECT 
	borrower_id, emp_title, emp_length, home_ownership, zip_code, addr_state, TO_DATE(earliest_cr_line, 'Mon-YYYY'),
	annual_inc, open_acc, pub_rec, dti, revol_bal, revol_util, total_acc, mort_acc, pub_rec_bankruptcies
	FROM accepted_loans
	
-- Check null values in loans table
SELECT
	COUNT(*) AS total_rows,
	COUNT(borrower_id) AS borrower_rows,
	COUNT(loan_amnt) AS loan_amnt_rows,
	COUNT(int_rate) AS int_rate_rows,
	COUNT(installment) AS installment_rows,
	COUNT(grade) AS grade_rows,
	COUNT(sub_grade) AS sub_grade_rows,
	COUNT(verification_status) AS verification_status_rows,
	COUNT(issue_d) AS issue_d_rows,
	COUNT(loan_status) AS loan_status_rows,
	COUNT(purpose) AS purpose_rows,
	COUNT(title) AS title_rows,
	COUNT(initial_list_status) AS initial_list_status_rows,
	COUNT(application_type) AS application_type_rows
FROM loans

-- Inspect null values
SELECT *
FROM loans as l
JOIN borrower as b
ON l.borrower_id = b.borrower_id
WHERE int_rate IS NULL

-- DELETE rows where int_rate IS NULL
DELETE FROM loans
WHERE int_rate IS NULL

-- Inspect null values in title column
SELECT *
FROM loans
WHERE title IS NULL

-- Populate null values in title as 'Not Applicable'
UPDATE loans
SET title = 'Not Applicable'
WHERE title IS NULL

-- Check null values in borrower table
SELECT
	COUNT(*) AS total_rows,
	COUNT(borrower_id) AS borrower_id_rows,
	COUNT(emp_title) AS emp_title_rows,
	COUNT(emp_length) AS emp_length_rows,
	COUNT(home_ownership) AS home_ownership_rows,
	COUNT(zip_code) AS zip_code_rows,
	COUNT(addr_state) AS addr_state_rows,
	COUNT(earliest_cr_line) AS earliest_cr_line_rows,
	COUNT(annual_inc) AS annual_inc_rows,
	COUNT(open_acc) AS open_acc_rows,
	COUNT(pub_rec) AS pub_rec_rows,
	COUNT(dti) AS dti_rows,
	COUNT(revol_bal) AS revol_bal_rows,
	COUNT(revol_util) AS revol_util_rows,
	COUNT(total_acc) AS total_acc_rows,
	COUNT(mort_acc) AS mort_acc_rows,
	COUNT(pub_rec_bankruptcies) AS pub_rec_bankruptcies_rows
FROM borrower

-- Delete row from borrower where borrower_id is not in loans table
DELETE FROM borrower
WHERE borrower_id = 
	(SELECT
		b.borrower_id
	FROM borrower as b
	LEFT JOIN loans as l
	ON b.borrower_id = l.borrower_id
	WHERE l.borrower_id IS NULL);

-- Inspect null values in emp_title column
SELECT *
FROM borrower
WHERE emp_title IS NULL

-- Update emp_title to not defined
UPDATE borrower
SET emp_title = 'Not Defined'
WHERE emp_title IS NULL

-- Inspect null values in emp_length column
SELECT *
FROM borrower
WHERE emp_length IS NULL

-- Impute null values in emp_length to 0
UPDATE borrower
SET emp_length = 0
WHERE emp_length IS NULL
)

-- Inspect null values in zip_code column
SELECT *
FROM borrower
WHERE zip_code IS NULL

-- Update zip_code to Not Available
UPDATE borrower
SET zip_code = 'Not Available'
WHERE zip_code IS NULL
FROM borrower

-- Inspect earlier_cr_line null values
SELECT *
FROM borrower
WHERE earliest_cr_line IS NULL

-- Update earliest_cr_line to 1900-01-01 means that no data is available
UPDATE borrower
SET earliest_cr_line = '1900-01-01'
WHERE earliest_cr_line IS NULL

-- Inspect annual_inc null values
SELECT *
FROM borrower
WHERE annual_inc IS NULL

-- Update annual_inc to 0 and don't delete rows since there is still loan amount on this borrower id
UPDATE borrower
SET annual_inc = 0
WHERE annual_inc IS NULL

-- Inspect open_acc null values
SELECT *
FROM borrower
WHERE open_acc IS NULL

-- Update open_acc null values to 0. The 0 value means that there's no open credit lines for this borrower
UPDATE borrower
SET open_acc = 0
WHERE open_acc IS NULL

-- Inspect pub_rec null values
SELECT *
FROM borrower
WHERE pub_rec IS NULL

-- Update pub_rec null values to 0. The 0 value means that there's no derogatory public records for this borrower
UPDATE borrower
SET pub_rec = 0
WHERE pub_rec IS NULL

-- Inspect dti null values
SELECT *
FROM borrower
WHERE dti IS NULL

-- Null values on dti rows corresponds to no annual inc. To maintain DOUBLE PRECISION data type in dti, I'll impute
-- it to 9999 to indicate that it is 'Undefined'
UPDATE borrower
SET dti = 9999
WHERE dti IS NULL

-- Inspect revol_util null values
SELECT *
FROM borrower
WHERE revol_util IS NULL

-- Null values in revol_util means 0. It could be the borrower is not using the borrowed money
UPDATE borrower
SET revol_util = 0
WHERE revol_util IS NULL

-- Inspect total_acc null values
SELECT *
FROM borrower
WHERE total_acc IS NULL

-- Set total_acc null values to 0 so that means there's no credit line currently opened.
UPDATE borrower
SET total_acc = 0
WHERE total_acc IS NULL

-- Inspect mort_acc null values
SELECT *
FROM borrower
WHERE mort_acc IS NULL

-- Setting null values in mort_acc to 0. It means that there's currently no opened mortgage accounts
UPDATE borrower
SET mort_acc = 0
WHERE mort_acc IS NULL AND home_ownership != 'MORTGAGE'

-- Setting null values in mort_acc to 1 if home_ownership = 'Mortgage' so that it means mort_acc >1.
UPDATE borrower
SET mort_acc = 1
WHERE mort_acc IS NULL AND home_ownership = 'MORTGAGE'

-- Inspect pub_rec_bankruptcies null values
SELECT *
FROM borrower
WHERE pub_rec_bankruptcies IS NULL

-- Setting pub_rec null values to 0 means that there's no public bankruptcies history on the borrower
UPDATE borrower
SET pub_rec_bankruptcies = 0
WHERE pub_rec_bankruptcies IS NULL

-- Charged off and Default is the same. Change default to charged off
UPDATE loans
SET loan_status = 'Charged Off'
WHERE loan_status = 'Default'

-- Export tables to CSV file
COPY loans TO 'D:/Project Portfolio/Leading_club_loan_dataset/loans_table.csv' WITH CSV HEADER;
COPY borrower TO 'D:/Project Portfolio/Leading_club_loan_dataset/borrower_table.csv' WITH CSV HEADER;
