import { useState, useEffect, useRef, useCallback } from 'react';
import { Sidebar } from '../components/Sidebar';

// ── Types ─────────────────────────────────────────────────────────────────────

interface QueryResponse {
  session_id: string;
  answer_text: string;
  columns: string[];
  rows: unknown[][];
  chart_spec: unknown | null;
  sql_executed: string | null;
  follow_ups: string[];
  confidence: number | null;
  limitations: string | null;
  generation_mode: string | null;
  execution_ms: number | null;
  assistant_message_id: number | null;
}

interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  data?: QueryResponse;
  loading?: boolean;
  error?: string;
}

interface SessionSummary {
  id: string;
  title: string;
  message_count: number;
  updated_at: string;
}

interface StoredMessage {
  id: number;
  role: 'user' | 'assistant';
  content: string;
  result: { columns?: string[]; rows?: unknown[][] } | null;
  meta: { feedback_helpful?: boolean } | null;
  created_at: string;
}

const SIDEBAR_KEY = 'mkc_sidebar_collapsed';

// ── API ───────────────────────────────────────────────────────────────────────

async function fetchSuggestedQuestions(): Promise<string[]> {
  const res = await fetch('/api/reporting/suggested-questions');
  if (!res.ok) return [];
  const data = await res.json();
  return data.questions ?? [];
}

async function fetchSessions(): Promise<SessionSummary[]> {
  const res = await fetch('/api/reporting/sessions');
  if (!res.ok) return [];
  const data = await res.json();
  return data.sessions ?? [];
}

async function fetchSession(sessionId: string): Promise<StoredMessage[]> {
  const res = await fetch(`/api/reporting/sessions/${sessionId}`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.messages ?? [];
}

async function sendQuery(question: string, sessionId: string | null): Promise<QueryResponse> {
  const body: Record<string, unknown> = { question };
  if (sessionId) body.session_id = sessionId;
  const res = await fetch('/api/reporting/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail ?? `API ${res.status}`);
  }
  return res.json();
}

async function sendFeedback(sessionId: string, messageId: number, helpful: boolean): Promise<void> {
  await fetch('/api/reporting/feedback', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: sessionId, message_id: messageId, helpful }),
  });
}

// ── Icons ─────────────────────────────────────────────────────────────────────

function IconSend() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="22" y1="2" x2="11" y2="13" />
      <polygon points="22 2 15 22 11 13 2 9 22 2" />
    </svg>
  );
}

function IconThumbUp({ filled }: { filled?: boolean }) {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill={filled ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M14 9V5a3 3 0 0 0-3-3l-4 9v11h11.28a2 2 0 0 0 2-1.7l1.38-9a2 2 0 0 0-2-2.3H14z" />
      <path d="M7 22H4a2 2 0 0 1-2-2v-7a2 2 0 0 1 2-2h3" />
    </svg>
  );
}

function IconThumbDown({ filled }: { filled?: boolean }) {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill={filled ? 'currentColor' : 'none'} stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M10 15v4a3 3 0 0 0 3 3l4-9V2H5.72a2 2 0 0 0-2 1.7l-1.38 9a2 2 0 0 0 2 2.3H10z" />
      <path d="M17 2h2.67A2.31 2.31 0 0 1 22 4v7a2.31 2.31 0 0 1-2.33 2H17" />
    </svg>
  );
}

function IconPlus() {
  return (
    <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="12" y1="5" x2="12" y2="19" /><line x1="5" y1="12" x2="19" y2="12" />
    </svg>
  );
}

function IconChevronDown() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="6 9 12 15 18 9" />
    </svg>
  );
}

function IconChevronUp() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="18 15 12 9 6 15" />
    </svg>
  );
}

// ── DataTable ─────────────────────────────────────────────────────────────────

function DataTable({ columns, rows }: { columns: string[]; rows: unknown[][] }) {
  if (columns.length === 0 || rows.length === 0) return null;
  return (
    <div className="mt-3 overflow-x-auto rounded-lg border border-border">
      <table className="w-full text-xs border-collapse">
        <thead>
          <tr className="bg-border/20">
            {columns.map((col) => (
              <th key={col} className="px-3 py-2 text-left text-muted font-medium whitespace-nowrap">
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.slice(0, 50).map((row, i) => (
            <tr key={i} className="border-t border-border/50 hover:bg-border/10 transition-colors">
              {(row as unknown[]).map((cell, j) => (
                <td key={j} className="px-3 py-2 text-ink whitespace-nowrap">
                  {cell == null ? '—' : String(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 50 && (
        <div className="px-3 py-1.5 text-xs text-muted border-t border-border">
          Showing 50 of {rows.length} rows
        </div>
      )}
    </div>
  );
}

// ── AssistantMessage ──────────────────────────────────────────────────────────

function AssistantMessage({
  msg,
  sessionId,
  onFollowUp,
}: {
  msg: Message;
  sessionId: string | null;
  onFollowUp: (q: string) => void;
}) {
  const [sqlOpen, setSqlOpen] = useState(false);
  const [feedback, setFeedback] = useState<boolean | null>(null);
  const [feedbackSent, setFeedbackSent] = useState(false);

  const data = msg.data;

  const handleFeedback = async (helpful: boolean) => {
    if (!sessionId || !data?.assistant_message_id || feedbackSent) return;
    setFeedback(helpful);
    setFeedbackSent(true);
    await sendFeedback(sessionId, data.assistant_message_id, helpful).catch(() => {});
  };

  if (msg.loading) {
    return (
      <div className="flex gap-3 items-start">
        <div className="w-7 h-7 rounded-full bg-gold/20 flex-shrink-0 flex items-center justify-center mt-0.5">
          <img src="/static/logo.png" alt="MKC" className="w-5 h-5 rounded-full object-cover" />
        </div>
        <div className="flex-1 pt-1">
          <div className="flex items-center gap-1.5 text-muted text-xs">
            <span className="animate-pulse">Thinking</span>
            <span className="flex gap-0.5">
              {[0, 1, 2].map((i) => (
                <span key={i} className="w-1 h-1 rounded-full bg-muted animate-bounce" style={{ animationDelay: `${i * 150}ms` }} />
              ))}
            </span>
          </div>
        </div>
      </div>
    );
  }

  if (msg.error) {
    return (
      <div className="flex gap-3 items-start">
        <div className="w-7 h-7 rounded-full bg-red-900/40 flex-shrink-0 flex items-center justify-center mt-0.5">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" className="text-red-400">
            <circle cx="12" cy="12" r="10" /><line x1="12" y1="8" x2="12" y2="12" /><line x1="12" y1="16" x2="12.01" y2="16" />
          </svg>
        </div>
        <div className="flex-1 pt-0.5">
          <p className="text-red-400 text-sm">{msg.error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex gap-3 items-start">
      {/* Avatar */}
      <div className="w-7 h-7 rounded-full overflow-hidden ring-1 ring-gold/30 flex-shrink-0 bg-white mt-0.5">
        <img src="/static/logo.png" alt="MKC" className="w-full h-full object-cover" />
      </div>

      <div className="flex-1 min-w-0">
        {/* Answer text */}
        <div className="text-ink text-sm leading-relaxed whitespace-pre-wrap">{msg.content}</div>

        {/* Data table */}
        {data && data.columns.length > 0 && data.rows.length > 0 && (
          <DataTable columns={data.columns} rows={data.rows} />
        )}

        {/* SQL expandable */}
        {data?.sql_executed && (
          <div className="mt-2">
            <button
              onClick={() => setSqlOpen((v) => !v)}
              className="flex items-center gap-1 text-xs text-muted/60 hover:text-muted transition-colors"
            >
              {sqlOpen ? <IconChevronUp /> : <IconChevronDown />}
              SQL
            </button>
            {sqlOpen && (
              <pre className="mt-1 text-xs text-muted/70 bg-border/10 border border-border rounded-lg p-3 overflow-x-auto">
                {data.sql_executed}
              </pre>
            )}
          </div>
        )}

        {/* Meta row: exec time + feedback */}
        <div className="flex items-center gap-3 mt-2.5">
          {data?.execution_ms != null && (
            <span className="text-muted/40 text-xs">{data.execution_ms < 1000 ? `${Math.round(data.execution_ms)}ms` : `${(data.execution_ms / 1000).toFixed(1)}s`}</span>
          )}

          {data?.assistant_message_id != null && sessionId && (
            <div className="flex items-center gap-1 ml-auto">
              <button
                onClick={() => void handleFeedback(true)}
                disabled={feedbackSent}
                className={`p-1 rounded-md transition-colors ${
                  feedback === true ? 'text-gold' : 'text-muted/40 hover:text-muted disabled:opacity-30'
                }`}
                title="Helpful"
              >
                <IconThumbUp filled={feedback === true} />
              </button>
              <button
                onClick={() => void handleFeedback(false)}
                disabled={feedbackSent}
                className={`p-1 rounded-md transition-colors ${
                  feedback === false ? 'text-red-400' : 'text-muted/40 hover:text-muted disabled:opacity-30'
                }`}
                title="Not helpful"
              >
                <IconThumbDown filled={feedback === false} />
              </button>
            </div>
          )}
        </div>

        {/* Follow-up suggestions */}
        {data?.follow_ups && data.follow_ups.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-3">
            {data.follow_ups.slice(0, 4).map((q, i) => (
              <button
                key={i}
                onClick={() => onFollowUp(q)}
                className="text-xs px-2.5 py-1 rounded-full border border-border text-muted hover:border-gold/40 hover:text-ink transition-colors"
              >
                {q}
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Sessions sidebar ──────────────────────────────────────────────────────────

function SessionsSidebar({
  sessions,
  activeSessionId,
  onSelect,
  onNew,
}: {
  sessions: SessionSummary[];
  activeSessionId: string | null;
  onSelect: (id: string) => void;
  onNew: () => void;
}) {
  return (
    <div className="flex flex-col h-full overflow-hidden" style={{ backgroundColor: '#060709', borderRight: '1px solid #1d2329' }}>
      <div className="flex items-center justify-between px-4 py-3 border-b border-border flex-shrink-0">
        <span className="text-muted text-xs uppercase tracking-widest">Sessions</span>
        <button
          onClick={onNew}
          title="New chat"
          className="text-muted hover:text-ink transition-colors p-1 rounded-md hover:bg-border/30"
        >
          <IconPlus />
        </button>
      </div>
      <div className="flex-1 overflow-y-auto py-2">
        {sessions.length === 0 ? (
          <div className="px-4 py-3 text-muted text-xs">No past sessions</div>
        ) : (
          sessions.map((s) => (
            <button
              key={s.id}
              onClick={() => onSelect(s.id)}
              className={`w-full text-left px-4 py-2.5 transition-colors ${
                activeSessionId === s.id
                  ? 'bg-gold/8 text-ink'
                  : 'text-muted hover:text-ink hover:bg-border/20'
              }`}
            >
              <div className="text-xs font-medium truncate">{s.title || 'Untitled'}</div>
              <div className="text-xs text-muted/50 mt-0.5">{s.message_count} messages</div>
            </button>
          ))
        )}
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

let msgCounter = 0;
function nextId() { return `msg-${++msgCounter}`; }

export default function Reporting() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState<boolean>(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true'
  );
  const [sessionsPanelOpen, setSessionsPanelOpen] = useState(false);
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [suggestions, setSuggestions] = useState<string[]>([]);

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  // Listen to sidebar toggle
  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent<{ collapsed: boolean }>;
      setSidebarCollapsed(ce.detail.collapsed);
    };
    window.addEventListener('mkc-sidebar-toggle', handler);
    return () => window.removeEventListener('mkc-sidebar-toggle', handler);
  }, []);

  // Load initial data
  useEffect(() => {
    fetchSuggestedQuestions().then(setSuggestions).catch(() => {});
    fetchSessions().then(setSessions).catch(() => {});
  }, []);

  // Scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSend = useCallback(async (question: string) => {
    const q = question.trim();
    if (!q || loading) return;

    const userMsg: Message = { id: nextId(), role: 'user', content: q };
    const loadingMsg: Message = { id: nextId(), role: 'assistant', content: '', loading: true };
    setMessages((prev) => [...prev, userMsg, loadingMsg]);
    setInput('');
    setLoading(true);

    try {
      const response = await sendQuery(q, sessionId);

      // Update session
      if (!sessionId || sessionId !== response.session_id) {
        setSessionId(response.session_id);
        fetchSessions().then(setSessions).catch(() => {});
      }

      const assistantMsg: Message = {
        id: nextId(),
        role: 'assistant',
        content: response.answer_text,
        data: response,
      };
      setMessages((prev) => [...prev.slice(0, -1), assistantMsg]);
    } catch (err) {
      const errorMsg: Message = {
        id: nextId(),
        role: 'assistant',
        content: '',
        error: err instanceof Error ? err.message : 'Something went wrong.',
      };
      setMessages((prev) => [...prev.slice(0, -1), errorMsg]);
    } finally {
      setLoading(false);
      inputRef.current?.focus();
    }
  }, [loading, sessionId]);

  const handleLoadSession = useCallback(async (id: string) => {
    setSessionId(id);
    setSessionsPanelOpen(false);
    const stored = await fetchSession(id).catch(() => [] as StoredMessage[]);
    const converted: Message[] = stored.map((m) => ({
      id: nextId(),
      role: m.role,
      content: m.content,
      data: m.role === 'assistant' && m.result
        ? {
            session_id: id,
            answer_text: m.content,
            columns: m.result.columns ?? [],
            rows: m.result.rows ?? [],
            chart_spec: null,
            sql_executed: null,
            follow_ups: [],
            confidence: null,
            limitations: null,
            generation_mode: null,
            execution_ms: null,
            assistant_message_id: m.id,
          }
        : undefined,
    }));
    setMessages(converted);
  }, []);

  const handleNewChat = useCallback(() => {
    setSessionId(null);
    setMessages([]);
    setSessionsPanelOpen(false);
    inputRef.current?.focus();
  }, []);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      void handleSend(input);
    }
  };

  const isEmpty = messages.length === 0;
  const marginClass = sidebarCollapsed ? 'ml-16' : 'ml-56';

  return (
    <div className="min-h-screen bg-surface">
      <Sidebar />

      <main className={`${marginClass} transition-[margin] duration-200 flex flex-col h-screen`}>
        {/* Top bar */}
        <div className="flex items-center justify-between px-8 py-4 border-b border-border flex-shrink-0">
          <div className="flex items-center gap-3">
            <h1 className="text-ink text-xl font-bold">Reporting</h1>
            {sessionId && (
              <span className="text-muted text-xs truncate max-w-[240px]">
                {sessions.find((s) => s.id === sessionId)?.title ?? ''}
              </span>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={handleNewChat}
              className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-border text-muted hover:text-ink hover:border-border/70 text-xs transition-colors"
            >
              <IconPlus />
              New chat
            </button>
            <button
              onClick={() => setSessionsPanelOpen((v) => !v)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs transition-colors ${
                sessionsPanelOpen
                  ? 'border-gold/40 text-gold bg-gold/5'
                  : 'border-border text-muted hover:text-ink hover:border-border/70'
              }`}
            >
              History
            </button>
          </div>
        </div>

        {/* Body */}
        <div className="flex flex-1 overflow-hidden">
          {/* Sessions panel */}
          <div className={`flex-shrink-0 transition-[width] duration-200 ${sessionsPanelOpen ? 'w-60' : 'w-0'} overflow-hidden`}>
            <SessionsSidebar
              sessions={sessions}
              activeSessionId={sessionId}
              onSelect={handleLoadSession}
              onNew={handleNewChat}
            />
          </div>

          {/* Chat area */}
          <div className="flex-1 flex flex-col overflow-hidden">
            {/* Messages */}
            <div className="flex-1 overflow-y-auto px-8 py-6">
              {isEmpty ? (
                // Welcome / suggestions
                <div className="max-w-xl mx-auto flex flex-col items-center gap-6 pt-12">
                  <div className="w-16 h-16 rounded-full overflow-hidden ring-2 ring-gold/30 bg-white">
                    <img src="/static/logo.png" alt="MKC" className="w-full h-full object-cover" />
                  </div>
                  <div className="text-center">
                    <h2 className="text-ink text-lg font-bold">Ask about your collection</h2>
                    <p className="text-muted text-sm mt-1">Natural language queries over your inventory and the MKC catalog.</p>
                  </div>
                  <div className="w-full flex flex-col gap-2">
                    {suggestions.map((q, i) => (
                      <button
                        key={i}
                        onClick={() => void handleSend(q)}
                        className="w-full text-left px-4 py-3 rounded-xl border border-border bg-card hover:border-gold/30 hover:bg-gold/5 transition-colors text-sm text-ink/80"
                      >
                        {q}
                      </button>
                    ))}
                  </div>
                </div>
              ) : (
                <div className="max-w-2xl mx-auto flex flex-col gap-6">
                  {messages.map((msg) => (
                    msg.role === 'user' ? (
                      // User message
                      <div key={msg.id} className="flex justify-end">
                        <div className="max-w-[80%] bg-gold/10 border border-gold/20 rounded-2xl rounded-tr-sm px-4 py-3 text-ink text-sm">
                          {msg.content}
                        </div>
                      </div>
                    ) : (
                      // Assistant message
                      <AssistantMessage
                        key={msg.id}
                        msg={msg}
                        sessionId={sessionId}
                        onFollowUp={(q) => void handleSend(q)}
                      />
                    )
                  ))}
                  <div ref={messagesEndRef} />
                </div>
              )}
            </div>

            {/* Input */}
            <div className="px-8 py-4 border-t border-border flex-shrink-0">
              <div className="max-w-2xl mx-auto">
                <div className="relative flex items-end gap-2 bg-card border border-border rounded-xl focus-within:border-gold/50 transition-colors">
                  <textarea
                    ref={inputRef}
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Ask about your collection…"
                    rows={1}
                    disabled={loading}
                    className="flex-1 px-4 py-3 bg-transparent resize-none text-sm text-ink placeholder:text-muted focus:outline-none min-h-[44px] max-h-32 disabled:opacity-50"
                    style={{ lineHeight: '1.5' }}
                  />
                  <button
                    onClick={() => void handleSend(input)}
                    disabled={!input.trim() || loading}
                    className="flex-shrink-0 m-2 w-8 h-8 rounded-lg bg-gold text-black flex items-center justify-center hover:bg-gold-bright disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
                  >
                    <IconSend />
                  </button>
                </div>
                <div className="text-center mt-1.5">
                  <span className="text-muted/40 text-xs">Enter to send · Shift+Enter for new line</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}
