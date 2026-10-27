-- Usage sums tokens of model calls completed since the start of the month.
CREATE INDEX steps_llm_completed_idx ON steps (completed_at) WHERE kind = 'llm';
