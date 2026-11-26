// Copyright 2026 Diviga
// SPDX-License-Identifier: Apache-2.0

'use client'

export interface DashboardEvent {
  id: string
  cursor: number
  type: string
  channel_id?: string
  channel: string
  timestamp: string | Date
  payload: Record<string, unknown>
  isState?: boolean
  state?: Record<string, unknown>
}

export const MOCK_CHANNELS = [
  'agent-research-run',
  'agent-research-run-exp-a',
  'agent-research-run-exp-b',
  'agent-research-run-exp-a-sub',
  'langgraph-agent-prod',
  'crewai-research-crew',
  'oncall-automation',
  'customer-support-bot',
  'document-summarizer',
  'data-pipeline-orchestrator',
]

/** Human display names for the demo channels (what `channel_metadata` resolves
 * to in a real instance). Used by the Runs list and the Forks laboratory. */
export const MOCK_CHANNEL_NAMES: Record<string, string> = {
  'agent-research-run': 'Research run · baseline',
  'agent-research-run-exp-a': 'Fork A · GPT-5.5 prompt',
  'agent-research-run-exp-b': 'Fork B · Claude 3.5',
  'agent-research-run-exp-a-sub': 'Fork A1 · temperature 0.9',
  'langgraph-agent-prod': 'LangGraph research · prod',
  'crewai-research-crew': 'CrewAI research crew',
  'oncall-automation': 'On-call incident automation',
  'customer-support-bot': 'Customer support bot',
  'document-summarizer': 'Document summarizer batch',
  'data-pipeline-orchestrator': 'Daily revenue ETL',
}

/** Approval lifecycle events so the Approvals page has a realistic mix of
 * pending / approved / rejected / expired requests in the demo. */
export const MOCK_APPROVAL_EVENTS: Record<string, unknown>[] = [
  { id: 'apr-deploy-req', cursor: 900, event_type: 'approval.requested', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-25T04:02:00Z', payload: { request_id: 'apr-deploy-gate', summary: 'Approve production deploy after 23 anomalies detected', details: { pipeline: 'daily_revenue_etl', anomalies: 23, data_quality_score: 99.991 }, timeout_seconds: 86400 } },
  { id: 'apr-refund-req', cursor: 901, event_type: 'approval.requested', channel_id: 'customer-support-bot', timestamp: '2026-07-25T14:00:09Z', payload: { request_id: 'apr-refund-4421', summary: 'Approve $499.99 refund for cust-4421 (duplicate charge)', details: { account: 'acc-7712', amount: 499.99, plan: 'enterprise' }, timeout_seconds: 3600 } },
  { id: 'apr-refund-dec', cursor: 902, event_type: 'approval.decided', channel_id: 'customer-support-bot', timestamp: '2026-07-25T14:03:12Z', payload: { request_id: 'apr-refund-4421', decision: 'approved', actor: 'ops@acme.example', reason: 'Verified duplicate charge' } },
  { id: 'apr-publish-req', cursor: 903, event_type: 'approval.requested', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-25T09:05:00Z', payload: { request_id: 'apr-publish-report', summary: 'Approve publishing the AI Market Report 2026 externally', details: { title: 'AI Market Report 2026', sections: 4, word_count: 2500 }, timeout_seconds: 7200 } },
  { id: 'apr-publish-dec', cursor: 904, event_type: 'approval.decided', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-25T09:20:00Z', payload: { request_id: 'apr-publish-report', decision: 'rejected', actor: 'legal@acme.example', reason: 'Needs legal review before publication' } },
  { id: 'apr-batch-req', cursor: 905, event_type: 'approval.requested', channel_id: 'document-summarizer', timestamp: '2026-07-25T10:05:00Z', payload: { request_id: 'apr-batch-retry', summary: 'Approve retrying failed OCR documents (capped)', details: { batch: 'batch-891' }, timeout_seconds: 1800 } },
  { id: 'apr-batch-exp', cursor: 906, event_type: 'approval.expired', channel_id: 'document-summarizer', timestamp: '2026-07-25T10:35:00Z', payload: { request_id: 'apr-batch-retry' } },
]

/** Durable `fork.started` records for the demo fork family, so the Forks
 * laboratory can reconstruct lineage exactly as a real instance would. */
export const MOCK_FORK_EVENTS: Record<string, unknown>[] = [
  { id: 'fork-evt-a', cursor: 100, event_type: 'fork.started', channel_id: 'agent-research-run-exp-a', timestamp: '2026-07-25T10:00:00Z', payload: { source_channel_id: 'agent-research-run', resolved_cursor: 3, requested_cursor: 3, reason: 'Test GPT-5.5 prompt + Gemini search', reproducibility: 'exact', display_name: 'Fork A · GPT-5.5 prompt' } },
  { id: 'fork-evt-b', cursor: 101, event_type: 'fork.started', channel_id: 'agent-research-run-exp-b', timestamp: '2026-07-25T11:00:00Z', payload: { source_channel_id: 'agent-research-run', resolved_cursor: 3, requested_cursor: 3, reason: 'Claude 3.5 comparison', reproducibility: 'exact', display_name: 'Fork B · Claude 3.5' } },
  { id: 'fork-evt-a-sub', cursor: 102, event_type: 'fork.started', channel_id: 'agent-research-run-exp-a-sub', timestamp: '2026-07-25T12:00:00Z', payload: { source_channel_id: 'agent-research-run-exp-a', resolved_cursor: 2, requested_cursor: 2, reason: 'Higher temperature', reproducibility: 'exact', display_name: 'Fork A1 · temperature 0.9' } },
]

export const MOCK_EVENTS: Record<string, Record<string, unknown>[]> = {
  //  agent-research-run: Root run with fork experiments 
  'agent-research-run': [
    { id: 'fork-001', cursor: 1, event_type: 'agent.started', channel_id: 'agent-research-run', timestamp: '2026-07-25T09:00:00Z', payload: { agent: 'research-v1', model: 'gpt-4', task: 'Analyze competitor landscape' } },
    { id: 'fork-002', cursor: 2, event_type: 'tool.web_search', channel_id: 'agent-research-run', timestamp: '2026-07-25T09:00:05Z', payload: { tool: 'web_search', query: 'top 10 AI startups 2026', results: 8, latency_ms: 1200 } },
    { id: 'fork-003', cursor: 3, event_type: 'tool.llm_call', channel_id: 'agent-research-run', timestamp: '2026-07-25T09:00:10Z', payload: { model: 'gpt-4', tokens: 1200, latency_ms: 3100, cost: 0.06 } },
    { id: 'fork-004', cursor: 4, event_type: 'tool.web_search', channel_id: 'agent-research-run', timestamp: '2026-07-25T09:00:15Z', payload: { tool: 'web_search', query: 'enterprise AI adoption 2026', results: 12, latency_ms: 980 } },
    { id: 'fork-005', cursor: 5, event_type: 'agent.completed', channel_id: 'agent-research-run', timestamp: '2026-07-25T09:00:25Z', payload: { agent: 'research-v1', duration_ms: 25000, tokens_total: 4200, cost_total: 0.18, findings: 15 } },
    { cursor: 5, isState: true, channel_id: 'agent-research-run', timestamp: '2026-07-25T09:00:26Z', state: {
      agent: 'research-v1', status: 'completed', model: 'gpt-4',
      findings: 15, sources: 20, tokens: 4200, cost: 0.18,
      messages: [
        { role: 'system', content: 'You are a research agent.' },
        { role: 'assistant', content: 'Research complete. Found 15 key findings about AI startups and enterprise adoption.' },
      ],
    }},
  ],
  //  Experiment A: forked at cursor 3, different prompt/modelt 
  'agent-research-run-exp-a': [
    { id: 'fork-a-01', cursor: 1, event_type: 'experiment.run_started', channel_id: 'agent-research-run-exp-a', timestamp: '2026-07-25T10:00:00Z', payload: { forked_from: 'agent-research-run', at_cursor: 3, experiment: 'GPT-5.5 prompt optimization' } },
    { id: 'fork-a-02', cursor: 2, event_type: 'tool.llm_call', channel_id: 'agent-research-run-exp-a', timestamp: '2026-07-25T10:00:05Z', payload: { model: 'gpt-5.5-flash', tokens: 800, latency_ms: 800, cost: 0.02, token_count: 600 } },
    { id: 'fork-a-03', cursor: 3, event_type: 'tool.web_search', channel_id: 'agent-research-run-exp-a', timestamp: '2026-07-25T10:00:10Z', payload: { tool: 'web_search', query: 'AI startup funding 2026', results: 15, latency_ms: 650, tokens: 300, cost: 0.01 } },
    { id: 'fork-a-04', cursor: 4, event_type: 'experiment.step_completed', channel_id: 'agent-research-run-exp-a', timestamp: '2026-07-25T10:00:15Z', payload: { model: 'gpt-5.5-flash', latency_ms: 800, latency_ms_total: 2100, tokens: 800, tokens_total: 1700, cost: 0.02 } },
    { cursor: 4, isState: true, channel_id: 'agent-research-run-exp-a', timestamp: '2026-07-25T10:00:16Z', state: {
      experiment: 'GPT-5.5 prompt optimization', model: 'gpt-5.5-flash',
      status: 'running', steps: 2, tokens: 1700, cost: 0.05, latency_ms: 2100,
      parent: 'agent-research-run', forked_at_cursor: 3,
    }},
  ],
  //  Experiment B: forked at cursor 3, different model 
  'agent-research-run-exp-b': [
    { id: 'fork-b-01', cursor: 1, event_type: 'experiment.run_started', channel_id: 'agent-research-run-exp-b', timestamp: '2026-07-25T11:00:00Z', payload: { forked_from: 'agent-research-run', at_cursor: 3, experiment: 'Claude 3.5 comparison' } },
    { id: 'fork-b-02', cursor: 2, event_type: 'tool.llm_call', channel_id: 'agent-research-run-exp-b', timestamp: '2026-07-25T11:00:08Z', payload: { model: 'claude-3.5-sonnet', tokens: 1100, latency_ms: 2500, cost: 0.033, token_count: 1100 } },
    { id: 'fork-b-03', cursor: 3, event_type: 'tool.web_search', channel_id: 'agent-research-run-exp-b', timestamp: '2026-07-25T11:00:15Z', payload: { tool: 'web_search', query: 'AI market size 2026 forecasts', results: 10, latency_ms: 1100, tokens: 200 } },
    { id: 'fork-b-04', cursor: 4, event_type: 'tool.llm_call', channel_id: 'agent-research-run-exp-b', timestamp: '2026-07-25T11:00:25Z', payload: { model: 'claude-3.5-sonnet', tokens: 900, latency_ms: 3000, cost: 0.027, token_count: 900 } },
    { cursor: 4, isState: true, channel_id: 'agent-research-run-exp-b', timestamp: '2026-07-25T11:00:26Z', state: {
      experiment: 'Claude 3.5 comparison', model: 'claude-3.5-sonnet',
      status: 'running', steps: 3, tokens: 2200, cost: 0.08, latency_ms: 6600,
      parent: 'agent-research-run', forked_at_cursor: 3,
    }},
  ],
  //  Sub-experiment A1: forked from A 
  'agent-research-run-exp-a-sub': [
    { id: 'fork-sub-01', cursor: 1, event_type: 'experiment.run_started', channel_id: 'agent-research-run-exp-a-sub', timestamp: '2026-07-25T12:00:00Z', payload: { forked_from: 'agent-research-run-exp-a', at_cursor: 2, experiment: 'Sub-experiment - different temperature' } },
    { id: 'fork-sub-02', cursor: 2, event_type: 'tool.llm_call', channel_id: 'agent-research-run-exp-a-sub', timestamp: '2026-07-25T12:00:04Z', payload: { model: 'gpt-5.5-flash', tokens: 500, latency_ms: 400, cost: 0.01, temperature: 0.9, token_count: 500 } },
    { cursor: 2, isState: true, channel_id: 'agent-research-run-exp-a-sub', timestamp: '2026-07-25T12:00:05Z', state: {
      experiment: 'Sub-experiment A1', model: 'gpt-5.5-flash',
      status: 'running', temperature: 0.9, steps: 1, tokens: 500, cost: 0.01,
      parent: 'agent-research-run-exp-a', forked_at_cursor: 2,
    }},
  ],
  //  langgraph-agent-prod: LangGraph agent with research → analysis → report pipeline 
  'langgraph-agent-prod': [
    { id: 'evt-001', cursor: 1, event_type: 'node.research.started', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:01Z', payload: { agent: 'research', config: { thread_id: 'th-001', model: 'gpt-4', temperature: 0.3, max_tokens: 4000 }, input: 'Analyze AI market trends 2026' } },
    { id: 'evt-002', cursor: 2, event_type: 'tool.web_search.called', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:03Z', payload: { tool: 'web_search', query: 'AI market trends 2026 enterprise adoption', result_count: 23, latency_ms: 845, sources: ['arxiv', 'bloomberg', 'techcrunch'] } },
    { id: 'evt-003', cursor: 3, event_type: 'tool.llm.called', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:05Z', payload: { model: 'gpt-4', tokens_in: 1200, tokens_out: 450, latency_ms: 2341, temperature: 0.3, operation: 'summarize_findings' } },
    { id: 'evt-004', cursor: 4, event_type: 'node.research.completed', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:07Z', payload: { agent: 'research', duration_ms: 6000, findings_count: 3, sources: ['arxiv:2305.18290', 'bloomberg:ai-report-2026', 'tc:agent-trends'] } },
    { id: 'evt-005', cursor: 5, event_type: 'node.analysis.started', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:08Z', payload: { agent: 'analysis', input_size_bytes: 4523, config: { model: 'gpt-4-turbo', depth: 'comprehensive' } } },
    { id: 'evt-006', cursor: 6, event_type: 'tool.llm.called', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:10Z', payload: { model: 'gpt-4-turbo', tokens_in: 3200, tokens_out: 890, latency_ms: 3120, operation: 'cross_reference_sources' } },
    // State snapshot at cursor 6
    { cursor: 6, isState: true, channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:11Z', state: {
      completed_nodes: ['research'],
      current_node: 'analysis',
      messages: [
        { role: 'system', content: 'You are a market research agent.' },
        { role: 'assistant', content: 'Research complete. Found 3 key trends in AI: enterprise agent adoption, RAG 2.0 architectures, and multimodal models.' },
      ],
      tool_outputs: [
        { tool: 'web_search', query: 'AI market trends 2026', results: 23, sources: ['arxiv', 'bloomberg', 'techcrunch'] },
        { tool: 'llm', model: 'gpt-4', tokens_used: 1650, operation: 'summarize_findings' },
      ],
      config: { thread_id: 'th-001', checkpoint_id: 'ckpt-3' },
      metrics: { total_tokens: 1650, total_cost_usd: 0.0825, duration_ms: 10000 },
    }},
    { id: 'evt-007', cursor: 7, event_type: 'node.analysis.completed', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:13Z', payload: { agent: 'analysis', duration_ms: 5000, trends: ['enterprise AI', 'agentic workflows', 'RAG 2.0'], risks: ['cost', 'latency', 'hallucination'] } },
    { id: 'evt-008', cursor: 8, event_type: 'node.report.started', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:14Z', payload: { agent: 'report', resumed: true, from_checkpoint: 'ckpt-3', template: 'executive_summary_v2' } },
    { id: 'evt-009', cursor: 9, event_type: 'agent.error', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:16Z', payload: { node: 'report', error: 'Rate limit exceeded on LLM API', retriable: true, retry_after_ms: 5000, attempt: 1 } },
    { id: 'evt-010', cursor: 10, event_type: 'agent.retry.started', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:21Z', payload: { from_cursor: 8, retry_attempt: 1, backoff_ms: 5000, strategy: 'exponential' } },
    { id: 'evt-011', cursor: 11, event_type: 'node.report.completed', channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:28Z', payload: { agent: 'report', duration_ms: 14000, title: 'AI Market Report 2026', sections: 4, word_count: 2500, format: 'markdown' } },
    // Final state snapshot at cursor 11
    { cursor: 11, isState: true, channel_id: 'langgraph-agent-prod', timestamp: '2026-07-24T09:00:29Z', state: {
      completed_nodes: ['research', 'analysis', 'report'],
      status: 'completed',
      final_report: { title: 'AI Market Report 2026', word_count: 2500, sections: 4, format: 'markdown' },
      total_llm_calls: 3,
      total_tokens: 5540,
      total_cost_usd: 0.277,
      execution_time_ms: 28000,
      artifacts: [
        { name: 'research_findings.json', size_bytes: 12500, type: 'json' },
        { name: 'analysis_report.md', size_bytes: 45000, type: 'markdown' },
        { name: 'executive_summary.pdf', size_bytes: 120000, type: 'pdf' },
      ],
    }},
  ],

  //  crewai-research-crew: CrewAI multi-agent research task pipeline 
  'crewai-research-crew': [
    { id: 'evt-101', cursor: 101, event_type: 'task.setup_environment.completed', channel_id: 'crewai-research-crew', timestamp: '2026-07-24T08:00:01Z', payload: { task: 'setup_environment', agent: 'researcher', output: 'Infrastructure configured', duration_ms: 1500 } },
    { id: 'evt-102', cursor: 102, event_type: 'task.research_market.completed', channel_id: 'crewai-research-crew', timestamp: '2026-07-24T08:00:05Z', payload: { task: 'research_market', agent: 'researcher', output: 'Market analysis complete', competitors: 5, market_size_billion: 42.5, growth_rate_pct: 18.3 } },
    // State at cursor 102 — crashed after 2 tasks
    { cursor: 102, isState: true, channel_id: 'crewai-research-crew', timestamp: '2026-07-24T08:00:06Z', state: {
      completed_tasks: ['setup_environment', 'research_market'],
      current_task: 'analyze_findings',
      task_outputs: [
        { task: 'setup_environment', output: 'Infrastructure configured', agent: 'researcher' },
        { task: 'research_market', output: 'Market analysis complete — 5 competitors found', agent: 'researcher', competitors: 5 },
      ],
      status: 'crashed_before_task_3',
      crash_reason: 'LLM API timeout on task initialization',
      crash_timestamp: '2026-07-24T08:00:06Z',
    }},
    { id: 'evt-103', cursor: 103, event_type: 'task.resume', channel_id: 'crewai-research-crew', timestamp: '2026-07-24T08:00:10Z', payload: { task: 'analyze_findings', resume_from: 102, recovery_strategy: 'retry_with_backoff' } },
    { id: 'evt-104', cursor: 104, event_type: 'task.analyze_findings.completed', channel_id: 'crewai-research-crew', timestamp: '2026-07-24T08:00:15Z', payload: { task: 'analyze_findings', agent: 'analyst', output: 'SWOT analysis done', opportunities: 7, threats: 3, strengths: 5, weaknesses: 2 } },
    { id: 'evt-105', cursor: 105, event_type: 'task.generate_report.completed', channel_id: 'crewai-research-crew', timestamp: '2026-07-24T08:00:20Z', payload: { task: 'generate_report', agent: 'writer', output: 'Executive summary published', pages: 12, format: 'pdf', includes_graphs: true } },
    // State at cursor 105 — all tasks done
    { cursor: 105, isState: true, channel_id: 'crewai-research-crew', timestamp: '2026-07-24T08:00:21Z', state: {
      completed_tasks: ['setup_environment', 'research_market', 'analyze_findings', 'generate_report'],
      status: 'completed',
      total_duration_ms: 20000,
      agents_used: ['researcher', 'analyst', 'writer'],
      artifacts: [
        { name: 'market_research.pdf', pages: 12, size_kb: 2400 },
        { name: 'swot_analysis.json', entries: 17 },
      ],
      metrics: { total_api_calls: 8, total_tokens: 12300, total_cost: 0.62 },
    }},
  ],

  //  oncall-automation: Incident response automation (no state snapshots) 
  'oncall-automation': [
    { id: 'evt-201', cursor: 201, event_type: 'incident.detected', channel_id: 'oncall-automation', timestamp: '2026-07-24T02:00:00Z', payload: { incident_id: 'inc-789', severity: 'critical', service: 'payment-api', region: 'us-east-1', error_rate_pct: 23.5, impact: 'payment_processing_down' } },
    { id: 'evt-202', cursor: 202, event_type: 'agent.paged', channel_id: 'oncall-automation', timestamp: '2026-07-24T02:00:01Z', payload: { oncall: 'sre-team', escalation_policy: 'tier-1', paged_via: ['pagerduty', 'slack'], responded_within_sec: 45 } },
    { id: 'evt-203', cursor: 203, event_type: 'runbook.started', channel_id: 'oncall-automation', timestamp: '2026-07-24T02:00:05Z', payload: { runbook: 'payment-api-degradation', step: 1, total_steps: 5, runbook_version: 'v3.2', triggered_by: 'inc-789' } },
    { id: 'evt-204', cursor: 204, event_type: 'runbook.progress', channel_id: 'oncall-automation', timestamp: '2026-07-24T02:00:30Z', payload: { runbook: 'payment-api-degradation', step: 3, action: 'restart_pod', target_pod: 'payment-api-v2-7f8b9c', confirmed: true, duration_ms: 25000 } },
    { id: 'evt-205', cursor: 205, event_type: 'runbook.completed', channel_id: 'oncall-automation', timestamp: '2026-07-24T02:02:00Z', payload: { runbook: 'payment-api-degradation', result: 'resolved', mtbf: 120, steps_completed: 5, error_rate_post: 0.2, resolution: 'restarted_pod_and_cleared_cache' } },
    { id: 'evt-206', cursor: 206, event_type: 'incident.resolved', channel_id: 'oncall-automation', timestamp: '2026-07-24T02:02:01Z', payload: { incident_id: 'inc-789', duration_min: 2.02, severity: 'critical', resolved_by: 'automation', postmortem_id: 'pm-789' } },
  ],

  //  customer-support-bot: Customer support conversation with escalation 
  'customer-support-bot': [
    { id: 'evt-301', cursor: 301, event_type: 'conversation.started', channel_id: 'customer-support-bot', timestamp: '2026-07-24T14:00:00Z', payload: { customer_id: 'cust-4421', channel: 'chat', language: 'en', topic: 'billing', sentiment: 'frustrated' } },
    { id: 'evt-302', cursor: 302, event_type: 'llm.response', channel_id: 'customer-support-bot', timestamp: '2026-07-24T14:00:03Z', payload: { model: 'gpt-4o-mini', latency_ms: 1200, response_type: 'greeting_and_empathy', tokens_used: 180 } },
    { id: 'evt-303', cursor: 303, event_type: 'tool.search_kb', channel_id: 'customer-support-bot', timestamp: '2026-07-24T14:00:05Z', payload: { query: 'refund policy enterprise plan', results: 3, top_result: 'KB-4421: Enterprise refund terms', confidence: 0.92 } },
    { id: 'evt-304', cursor: 304, event_type: 'tool.lookup_account', channel_id: 'customer-support-bot', timestamp: '2026-07-24T14:00:06Z', payload: { account_id: 'acc-7712', plan: 'enterprise', tenure_months: 14, billing_issue: 'duplicate_charge_2026-07-20', amount: 499.99 } },
    { id: 'evt-305', cursor: 305, event_type: 'agent.escalated', channel_id: 'customer-support-bot', timestamp: '2026-07-24T14:00:10Z', payload: { reason: 'refund_requires_approval', escalated_to: 'billing_team', priority: 'high', conversation_id: 'conv-331', duration_ms: 10000 } },
    { id: 'evt-306', cursor: 306, event_type: 'conversation.ended', channel_id: 'customer-support-bot', timestamp: '2026-07-24T14:00:15Z', payload: { resolution: 'escalated', csat_score: null, duration_sec: 15, messages_count: 6 } },
    { cursor: 306, isState: true, channel_id: 'customer-support-bot', timestamp: '2026-07-24T14:00:16Z', state: {
      customer: { id: 'cust-4421', plan: 'enterprise', tenure_months: 14 },
      issue: { type: 'billing', detail: 'duplicate_charge', amount: 499.99, status: 'escalated' },
      conversation: { id: 'conv-331', message_count: 6, duration_sec: 15, sentiment_history: ['frustrated', 'neutral', 'hopeful'] },
      resolution: { action: 'escalated_to_billing_team', refund_approved: null, follow_up_required: true },
    }},
  ],

  //  document-summarizer: Batch document processing pipeline 
  'document-summarizer': [
    { id: 'evt-401', cursor: 401, event_type: 'batch.started', channel_id: 'document-summarizer', timestamp: '2026-07-24T10:00:00Z', payload: { batch_id: 'batch-891', document_count: 12, total_pages: 345, model: 'gpt-4o', priority: 'high' } },
    { id: 'evt-402', cursor: 402, event_type: 'document.processing', channel_id: 'document-summarizer', timestamp: '2026-07-24T10:00:05Z', payload: { document_id: 'doc-01', filename: 'Q2_financial_report.pdf', pages: 42, progress_pct: 15 } },
    { id: 'evt-403', cursor: 403, event_type: 'tool.ocr.completed', channel_id: 'document-summarizer', timestamp: '2026-07-24T10:00:10Z', payload: { document_id: 'doc-01', pages_processed: 42, characters_extracted: 158000, accuracy_pct: 99.2, latency_ms: 5000 } },
    { id: 'evt-404', cursor: 404, event_type: 'llm.summary.completed', channel_id: 'document-summarizer', timestamp: '2026-07-24T10:00:15Z', payload: { document_id: 'doc-01', tokens_in: 158000, tokens_out: 1200, summary_length: 'detailed', key_points: 8, latency_ms: 8500 } },
    { id: 'evt-405', cursor: 405, event_type: 'document.completed', channel_id: 'document-summarizer', timestamp: '2026-07-24T10:00:16Z', payload: { document_id: 'doc-01', status: 'completed', duration_ms: 16000, output_format: 'markdown', size_bytes: 4500 } },
    { id: 'evt-406', cursor: 406, event_type: 'batch.progress', channel_id: 'document-summarizer', timestamp: '2026-07-24T10:00:20Z', payload: { batch_id: 'batch-891', processed: 1, total: 12, progress_pct: 8.3, eta_seconds: 180 } },
    { cursor: 406, isState: true, channel_id: 'document-summarizer', timestamp: '2026-07-24T10:00:21Z', state: {
      batch: { id: 'batch-891', total_documents: 12, processed: 1, failed: 0 },
      current: { document_id: 'doc-01', status: 'completed' },
      queue: ['doc-02', 'doc-03', 'doc-04', 'doc-05', 'doc-06', 'doc-07', 'doc-08', 'doc-09', 'doc-10', 'doc-11', 'doc-12'],
      metrics: { avg_processing_time_ms: 16000, tokens_used_so_far: 159200, estimated_total_tokens: 1900000 },
    }},
  ],

  //  data-pipeline-orchestrator: ETL pipeline with multiple stages 
  'data-pipeline-orchestrator': [
    { id: 'evt-501', cursor: 501, event_type: 'pipeline.triggered', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:00:00Z', payload: { pipeline: 'daily_revenue_etl', schedule: 'cron(0 4 * * *)', source: 'postgres:analytics', target: 's3://data-lake/revenue/' } },
    { id: 'evt-502', cursor: 502, event_type: 'stage.extract.started', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:00:01Z', payload: { stage: 'extract', table: 'transactions', row_count_estimate: 2500000, batch_size: 10000 } },
    { id: 'evt-503', cursor: 503, event_type: 'stage.extract.completed', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:00:30Z', payload: { stage: 'extract', rows_extracted: 2483921, duration_ms: 29000, throughput_rows_per_sec: 85652 } },
    { id: 'evt-504', cursor: 504, event_type: 'stage.transform.started', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:00:31Z', payload: { stage: 'transform', transformations: ['clean_timestamps', 'enrich_geo', 'calculate_mrr', 'flag_anomalies'], parallel_workers: 8 } },
    { id: 'evt-505', cursor: 505, event_type: 'stage.transform.completed', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:01:15Z', payload: { stage: 'transform', duration_ms: 44000, rows_transformed: 2483921, anomalies_detected: 23 } },
    { id: 'evt-506', cursor: 506, event_type: 'stage.load.started', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:01:16Z', payload: { stage: 'load', target: 's3://data-lake/revenue/2026/07/24/', format: 'parquet', compression: 'snappy' } },
    { id: 'evt-507', cursor: 507, event_type: 'stage.load.completed', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:01:45Z', payload: { stage: 'load', duration_ms: 29000, files_written: 24, total_size_mb: 892, throughput_mbps: 30.8 } },
    { id: 'evt-508', cursor: 508, event_type: 'pipeline.completed', channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:01:46Z', payload: { pipeline: 'daily_revenue_etl', total_duration_ms: 106000, status: 'success', records_processed: 2483921, data_quality_score: 99.991 } },
    { cursor: 508, isState: true, channel_id: 'data-pipeline-orchestrator', timestamp: '2026-07-24T04:01:47Z', state: {
      pipeline: { id: 'daily_revenue_etl', status: 'completed', schedule: 'daily' },
      stages: {
        extract: { status: 'completed', rows: 2483921, duration_ms: 29000 },
        transform: { status: 'completed', transformations: 4, duration_ms: 44000 },
        load: { status: 'completed', files: 24, size_mb: 892, duration_ms: 29000 },
      },
      metrics: { total_duration_ms: 106000, total_rows: 2483921, cost_estimate_usd: 1.24 },
      data_quality: { score: 99.991, anomalies: 23, warnings: 0 },
      artifacts: [
        { name: 'revenue_20260724.parquet', size_mb: 892, row_count: 2483921 },
        { name: 'execution_report.json', size_kb: 12 },
        { name: 'anomaly_log.csv', anomalies: 23 },
      ],
    }},
  ],
}

export const MOCK_STATES: Record<string, { cursor: number; state: Record<string, unknown> } | null> = {
  'agent-research-run': {
    cursor: 5,
    state: {
      agent: 'research-v1', status: 'completed', model: 'gpt-4',
      findings: 15, tokens: 4200, cost: 0.18,
    },
  },
  'agent-research-run-exp-a': {
    cursor: 4,
    state: {
      experiment: 'GPT-5.5 prompt optimization', model: 'gpt-5.5-flash',
      status: 'running', tokens: 1700, cost: 0.05, parent: 'agent-research-run',
    },
  },
  'agent-research-run-exp-b': {
    cursor: 4,
    state: {
      experiment: 'Claude 3.5 comparison', model: 'claude-3.5-sonnet',
      status: 'running', tokens: 2200, cost: 0.08, parent: 'agent-research-run',
    },
  },
  'agent-research-run-exp-a-sub': {
    cursor: 2,
    state: {
      experiment: 'Sub-experiment A1', model: 'gpt-5.5-flash', temperature: 0.9,
      status: 'running', tokens: 500, cost: 0.01, parent: 'agent-research-run-exp-a',
    },
  },
  'langgraph-agent-prod': {
    cursor: 11,
    state: {
      completed_nodes: ['research', 'analysis', 'report'],
      status: 'completed',
      final_report: { title: 'AI Market Report 2026', word_count: 2500, sections: 4, format: 'markdown' },
      total_llm_calls: 3,
      total_tokens: 5540,
      total_cost_usd: 0.277,
      execution_time_ms: 28000,
      artifacts: [
        { name: 'research_findings.json', size_bytes: 12500 },
        { name: 'analysis_report.md', size_bytes: 45000 },
        { name: 'executive_summary.pdf', size_bytes: 120000 },
      ],
    },
  },
  'crewai-research-crew': {
    cursor: 105,
    state: {
      completed_tasks: ['setup_environment', 'research_market', 'analyze_findings', 'generate_report'],
      status: 'completed',
      total_duration_ms: 20000,
      agents_used: ['researcher', 'analyst', 'writer'],
      artifacts: [
        { name: 'market_research.pdf', pages: 12, size_kb: 2400 },
        { name: 'swot_analysis.json', entries: 17 },
      ],
    },
  },
  'oncall-automation': null,
  'customer-support-bot': {
    cursor: 306,
    state: {
      customer: { id: 'cust-4421', plan: 'enterprise', tenure_months: 14 },
      issue: { type: 'billing', detail: 'duplicate_charge', amount: 499.99, status: 'escalated' },
      conversation: { id: 'conv-331', message_count: 6, duration_sec: 15 },
      resolution: { action: 'escalated_to_billing_team', refund_approved: null, follow_up_required: true },
    },
  },
  'document-summarizer': {
    cursor: 406,
    state: {
      batch: { id: 'batch-891', total_documents: 12, processed: 1, failed: 0 },
      current: { document_id: 'doc-01', status: 'completed' },
      queue: ['doc-02', 'doc-03', 'doc-04', 'doc-05', 'doc-06', 'doc-07', 'doc-08', 'doc-09', 'doc-10', 'doc-11', 'doc-12'],
      metrics: { avg_processing_time_ms: 16000, tokens_used_so_far: 159200 },
    },
  },
  'data-pipeline-orchestrator': {
    cursor: 508,
    state: {
      pipeline: { id: 'daily_revenue_etl', status: 'completed', schedule: 'daily' },
      stages: {
        extract: { status: 'completed', rows: 2483921, duration_ms: 29000 },
        transform: { status: 'completed', transformations: 4, duration_ms: 44000 },
        load: { status: 'completed', files: 24, size_mb: 892, duration_ms: 29000 },
      },
      metrics: { total_duration_ms: 106000, total_rows: 2483921, cost_estimate_usd: 1.24 },
    },
  },
}

export function generateMockLiveEvent(): DashboardEvent {
  const channels = ['langgraph-agent-prod', 'crewai-research-crew', 'customer-support-bot', 'data-pipeline-orchestrator']
  const eventTemplates = [
    { type: 'node.heartbeat', payload: { status: 'alive', memory_mb: 128, cpu_pct: 23.5, active_agents: 2 } },
    { type: 'tool.llm.called', payload: { model: 'gpt-4', tokens_in: Math.floor(Math.random() * 500) + 100, tokens_out: Math.floor(Math.random() * 200) + 20, latency_ms: Math.floor(Math.random() * 3000) + 200 } },
    { type: 'tool.api_response', payload: { endpoint: '/api/v2/process', status_code: 200, latency_ms: Math.floor(Math.random() * 500) + 50, retry_count: 0 } },
    { type: 'agent.token_used', payload: { token_type: 'input', count: Math.floor(Math.random() * 1000) + 50, model: 'gpt-4o-mini' } },
    { type: 'metric.recorded', payload: { metric: 'response_time_ms', value: Math.floor(Math.random() * 500) + 100, unit: 'ms', tags: { env: 'prod', region: 'us-east-1' } } },
    { type: 'cache.hit', payload: { cache_name: 'llm_responses', key: `cache:${Math.random().toString(36).slice(2, 8)}`, hit_rate: 0.87, size_bytes: 45000 } },
    { type: 'node.progress', payload: { node: ['research', 'analysis', 'report', 'extract', 'transform'][Math.floor(Math.random() * 5)], progress_pct: Math.floor(Math.random() * 100), eta_ms: Math.floor(Math.random() * 10000) } },
    { type: 'log.info', payload: { message: 'Processing batch completed', batch_id: `batch-${Math.floor(Math.random() * 1000)}`, correlation_id: Math.random().toString(36).slice(2, 10) } },
  ]

  const template = eventTemplates[Math.floor(Math.random() * eventTemplates.length)]
  return {
    id: `mock-live-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    cursor: Math.floor(Math.random() * 10000) + 500,
    type: template.type,
    channel: channels[Math.floor(Math.random() * channels.length)],
    timestamp: new Date(),
    payload: template.payload,
    isState: false,
  }
}
