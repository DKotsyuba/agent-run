-- One optional human display label per agent, admitted with the request and
-- inherited by explicit resumes that do not name a replacement. The label is
-- validated at the admission boundary (nonblank, at most 64 Unicode scalar
-- values, no control, bidi or format characters) and confers no authority;
-- NULL is the ordinary unnamed state and historical rows stay NULL.
ALTER TABLE agents ADD COLUMN display_name TEXT;
