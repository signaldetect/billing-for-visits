INSERT INTO visits.visits (id, start_date_time, end_date_time, rate, status, description) VALUES
  ('00000000-0000-0000-0000-000000000001', '2026-11-11T08:00Z', '2026-11-11T09:30Z', 40.00, 'in_progress', 'Seed visit in progress'),
  ('00000000-0000-0000-0000-000000000002', '2026-11-11T08:00Z', '2026-11-11T09:30Z', 40.00, 'scheduled', 'Seed visit scheduled'),
  ('00000000-0000-0000-0000-000000000003', '2026-11-11T08:00Z', '2026-11-11T09:30Z', 40.00, 'completed', 'Seed visit completed'),
  ('00000000-0000-0000-0000-000000000004', '2026-11-11T08:00Z', '2026-11-11T09:30Z', 40.00, 'canceled', 'Seed visit canceled'),
  ('00000000-0000-0000-0000-000000000005', '2026-11-11T08:00Z', '2026-11-11T09:30Z', 40.00, 'missed', 'Seed visit missed')
ON CONFLICT (id) DO NOTHING;
