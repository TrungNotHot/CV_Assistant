-- =============================================
-- Enable required PostgreSQL extensions
-- =============================================
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- =============================================
-- Table: jobs
-- =============================================
CREATE TABLE IF NOT EXISTS jobs (
    id SERIAL PRIMARY KEY,
    job_title TEXT NOT NULL,
    location TEXT NOT NULL,
    seniority_level TEXT NOT NULL,
    posted_date DATE,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    job_title_tsv tsvector
);

-- Indexes for job search/filter
CREATE INDEX IF NOT EXISTS idx_jobs_title_tsv ON jobs USING GIN(job_title_tsv); -- fulltext
CREATE INDEX IF NOT EXISTS idx_jobs_title_trgm ON jobs USING GIN(job_title gin_trgm_ops); -- fuzzy
CREATE INDEX IF NOT EXISTS idx_jobs_location ON jobs(location);
CREATE INDEX IF NOT EXISTS idx_jobs_seniority ON jobs(seniority_level);

-- Auto-update tsvector for full-text search
CREATE OR REPLACE FUNCTION update_jobs_tsvector() RETURNS trigger AS $$
BEGIN
  NEW.job_title_tsv := to_tsvector('simple', unaccent(NEW.job_title));
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Re-create trigger safely
DROP TRIGGER IF EXISTS trg_jobs_tsv ON jobs;
CREATE TRIGGER trg_jobs_tsv
BEFORE INSERT OR UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION update_jobs_tsvector();

-- =============================================
-- Table: skills
-- =============================================
CREATE TABLE IF NOT EXISTS skills (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL CHECK (type IN ('soft', 'tech'))
);
CREATE INDEX IF NOT EXISTS idx_skills_type ON skills(type);

-- =============================================
-- Table: job_skills
-- =============================================
CREATE TABLE IF NOT EXISTS job_skills (
    job_id INT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    skill_id INT NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
    PRIMARY KEY (job_id, skill_id)
);
CREATE INDEX IF NOT EXISTS idx_job_skills_job_id ON job_skills(job_id);
CREATE INDEX IF NOT EXISTS idx_job_skills_skill_id ON job_skills(skill_id);

-- -- =============================================
-- -- Materialized View: skill_frequency_stats
-- -- Purpose: Pre-aggregated skill frequency for fast dashboard querying
-- -- =============================================

-- -- Drop old version (if exists) to allow structure updates
-- DROP MATERIALIZED VIEW IF EXISTS skill_frequency_stats;

-- -- Create fresh version with fixed window aggregation
-- CREATE MATERIALIZED VIEW skill_frequency_stats AS
-- WITH job_counts AS (
--   SELECT 
--     job_title,
--     location,
--     seniority_level,
--     COUNT(DISTINCT id) AS total_jobs
--   FROM jobs
--   GROUP BY job_title, location, seniority_level
-- )
-- SELECT
--   j.job_title,
--   j.location,
--   j.seniority_level,
--   s.name AS skill_name,
--   s.type AS skill_type,
--   COUNT(*) AS skill_count,
--   ROUND(COUNT(*) * 100.0 / jc.total_jobs, 1) AS frequency_percent
-- FROM job_skills js
-- JOIN jobs j ON js.job_id = j.id
-- JOIN skills s ON js.skill_id = s.id
-- JOIN job_counts jc ON
--   j.job_title = jc.job_title AND
--   j.location = jc.location AND
--   j.seniority_level = jc.seniority_level
-- GROUP BY
--   j.job_title,
--   j.location,
--   j.seniority_level,
--   s.name,
--   s.type,
--   jc.total_jobs;


-- -- Indexes to speed up filtering on view
-- CREATE INDEX IF NOT EXISTS idx_skill_freq_view ON skill_frequency_stats (
--   job_title, location, seniority_level, skill_type
-- );
-- CREATE UNIQUE INDEX IF NOT EXISTS idx_skill_freq_unique ON skill_frequency_stats (
--   job_title, location, seniority_level, skill_name
-- );

-- Create alias table for job titles
CREATE TABLE IF NOT EXISTS keyword_alias (
    job_word TEXT NOT NULL,
    keyword TEXT NOT NULL,
    PRIMARY KEY (job_word, keyword)
);

INSERT INTO keyword_alias (job_word, keyword) VALUES
('Software Engineer', 'swe'),
('Software Engineer', 'se'),
('Backend Developer', 'be'),
('Frontend Developer', 'fe'),
('Fullstack Developer', 'fsd'),
('DevOps Engineer', 'dev ops'),
('QA/QC Engineer', 'qa'),
('QA/QC Engineer', 'qc'),
('Automation Tester', 'auto tester'),
('Performance QA Engineer', 'perf tester'),
('Mobile Developer (Android/iOS/Flutter)', 'flutter dev'),
('Mobile Developer (Android/iOS/Flutter)', 'ios dev'),
('Mobile Developer (Android/iOS/Flutter)', 'android dev'),
('Data Engineer', 'de'),
('Data Scientist', 'ds'),
('System Administrator', 'sysadmin'),
('System Administrator', 'system admin'),
('Network Engineer', 'neteng'),
('Project Manager (IT PM)', 'pm'),
('UI/UX Designer', 'ux designer'),
('Business Analyst (BA)', 'ba'),
('IT Support / Helpdesk', 'helpdesk'),
('Cybersecurity Engineer', 'cybersec'),
('Cybersecurity Engineer', 'infosec'),
('Cybersecurity Engineer', 'pentester');
