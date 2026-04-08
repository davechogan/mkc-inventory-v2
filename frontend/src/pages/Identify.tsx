import { useState, useEffect, useCallback } from 'react';
import { Sidebar } from '../components/Sidebar';

// ── Types ─────────────────────────────────────────────────────────────────────

interface WizardQuestion {
  key: string;
  display_text: string;
  type: 'boolean' | 'single_choice' | 'multi_choice';
  options: { value: any; label: string; color?: string }[] | null;
  visual_aid: string | null;
}

interface WizardCandidate {
  model_id: number;
  name: string;
  family: string | null;
  form: string | null;
  handle_type: string | null;
  blade_length: number | null;
  blade_steel: string | null;
  blade_finish: string | null;
  msrp: number | null;
  best_colorway_id: number | null;
  has_image: boolean;
}

interface AnsweredQuestion {
  key: string;
  display_text: string;
  answer: any;
  answerLabel: string;
}

interface BladeFormSilhouette {
  id: number;
  name: string;
  slug: string;
  has_silhouette: boolean;
  image_url: string | null;
}

// ── API helpers ───────────────────────────────────────────────────────────────

async function wizardStart(): Promise<any> {
  const res = await fetch('/api/v2/identify/wizard/start', { method: 'POST' });
  if (!res.ok) throw new Error(`Start failed: ${res.status}`);
  return res.json();
}

async function wizardAnswer(sessionId: string, questionKey: string, answer: any): Promise<any> {
  const res = await fetch('/api/v2/identify/wizard/answer', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: sessionId, question_key: questionKey, answer }),
  });
  if (!res.ok) throw new Error(`Answer failed: ${res.status}`);
  return res.json();
}

async function wizardBack(sessionId: string): Promise<any> {
  const res = await fetch('/api/v2/identify/wizard/back', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: sessionId }),
  });
  if (!res.ok) throw new Error(`Back failed: ${res.status}`);
  return res.json();
}

async function fetchBladeFormSilhouettes(): Promise<BladeFormSilhouette[]> {
  const res = await fetch('/api/v2/blade-forms/silhouettes');
  if (!res.ok) return [];
  return res.json();
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ProgressBar({
  totalModels,
  remainingModels,
  remainingFamilies,
}: {
  totalModels: number;
  remainingModels: number;
  remainingFamilies: number;
}) {
  const pct = totalModels > 0 ? Math.max(2, (remainingModels / totalModels) * 100) : 100;
  return (
    <div className="px-6 py-3 border-b border-border bg-surface/50">
      <div className="flex items-center justify-between text-xs text-muted mb-1.5">
        <span>{remainingModels} models remaining</span>
        <span>{remainingFamilies} families</span>
      </div>
      <div className="h-1.5 bg-border/30 rounded-full overflow-hidden">
        <div
          className="h-full bg-gold rounded-full transition-all duration-500"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

function AnsweredList({ answered }: { answered: AnsweredQuestion[] }) {
  if (answered.length === 0) return null;
  const shorten = (text: string) =>
    text.split('?')[0]
      .replace('Is the handle wrapped in paracord or cord', 'Paracord')
      .replace('Is this a kitchen/culinary knife or a field/hunting knife', 'Type')
      .replace('Is there a finger ring at the front of the handle', 'Finger ring')
      .replace('Is this a hatchet or axe', 'Hatchet')
      .replace('Is the blade wider/taller than the handle (like a cleaver)', 'Cleaver')
      .replace('What color is the blade', 'Blade')
      .replace('What is the primary handle color', 'Handle color')
      .replace('What is the handle material', 'Material')
      .replace('Approximately how long is the blade', 'Length')
      .replace('Which blade shape(s) match your knife', 'Form');
  return (
    <div className="px-6 py-2 border-b border-border/50">
      <div className="flex flex-wrap gap-x-4 gap-y-1">
        {answered.map((a) => (
          <span key={a.key} className="text-xs text-muted">
            <span className="text-ink/60">{shorten(a.display_text)}:</span>{' '}
            <span className="text-gold">{a.answerLabel}</span>
          </span>
        ))}
      </div>
    </div>
  );
}

function SkipButton({ onClick, loading }: { onClick: () => void; loading: boolean }) {
  return (
    <button
      onClick={onClick}
      disabled={loading}
      className="text-xs text-muted/60 hover:text-muted transition-colors disabled:opacity-40 mt-1"
    >
      I don't know — skip this question
    </button>
  );
}

function BooleanQuestion({
  question, onAnswer, onSkip, loading,
}: {
  question: WizardQuestion;
  onAnswer: (answer: boolean) => void;
  onSkip: () => void;
  loading: boolean;
}) {
  return (
    <div className="flex flex-col items-center gap-6 py-6 px-6">
      <h2 className="text-lg font-semibold text-ink text-center">{question.display_text}</h2>
      <div className="flex gap-4">
        <button onClick={() => onAnswer(true)} disabled={loading}
          className="px-8 py-4 rounded-xl text-base font-semibold transition-colors border-2 border-border bg-card text-ink hover:border-gold/50 hover:bg-gold/5 disabled:opacity-40">
          Yes
        </button>
        <button onClick={() => onAnswer(false)} disabled={loading}
          className="px-8 py-4 rounded-xl text-base font-semibold transition-colors border-2 border-border bg-card text-ink hover:border-gold/50 hover:bg-gold/5 disabled:opacity-40">
          No
        </button>
      </div>
      <SkipButton onClick={onSkip} loading={loading} />
    </div>
  );
}

function ChoiceQuestion({
  question, onAnswer, onSkip, loading,
}: {
  question: WizardQuestion;
  onAnswer: (answer: any) => void;
  onSkip: () => void;
  loading: boolean;
}) {
  const options = question.options || [];
  const isColorQuestion = question.key === 'blade_color' || question.key === 'handle_color';

  return (
    <div className="flex flex-col items-center gap-6 py-6 px-6">
      <h2 className="text-lg font-semibold text-ink text-center">{question.display_text}</h2>
      {isColorQuestion ? (
        <div className="flex flex-wrap justify-center gap-3">
          {options.map((opt) => (
            <button key={String(opt.value)} onClick={() => onAnswer(opt.value)} disabled={loading}
              className="flex flex-col items-center gap-1.5 px-3 py-2 rounded-xl border border-border bg-card hover:border-gold/50 hover:bg-gold/5 transition-colors disabled:opacity-40">
              {opt.color && (
                <div className="w-8 h-8 rounded-full border border-border/50" style={{ backgroundColor: opt.color }} />
              )}
              <span className="text-xs text-ink font-medium">{opt.label}</span>
            </button>
          ))}
        </div>
      ) : (
        <div className="flex flex-col gap-2 w-full max-w-sm">
          {options.map((opt) => (
            <button key={String(opt.value)} onClick={() => onAnswer(opt.value)} disabled={loading}
              className="w-full px-4 py-3 rounded-xl border border-border bg-card text-left text-sm text-ink font-medium hover:border-gold/50 hover:bg-gold/5 transition-colors disabled:opacity-40">
              {opt.label}
            </button>
          ))}
        </div>
      )}
      <SkipButton onClick={onSkip} loading={loading} />
    </div>
  );
}

function BladeFormQuestion({
  question, silhouettes, onAnswer, onSkip, loading,
}: {
  question: WizardQuestion;
  silhouettes: BladeFormSilhouette[];
  onAnswer: (answer: string[]) => void;
  onSkip: () => void;
  loading: boolean;
}) {
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [zoomed, setZoomed] = useState<string | null>(null);

  const toggle = (name: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(name)) next.delete(name);
      else next.add(name);
      return next;
    });
  };

  const formsWithImages = silhouettes.filter((s) => s.has_silhouette);

  return (
    <div className="flex flex-col items-center gap-4 py-6 px-6">
      <h2 className="text-lg font-semibold text-ink text-center">{question.display_text}</h2>
      <p className="text-xs text-muted">Tap a shape to select it. Long-press or right-click to zoom in.</p>

      {zoomed && (
        <div className="fixed inset-0 z-50 bg-black/80 flex items-center justify-center p-8"
          onClick={() => setZoomed(null)}>
          <div className="bg-card rounded-2xl p-6 max-w-sm w-full flex flex-col items-center gap-3">
            <img src={formsWithImages.find(s => s.name === zoomed)?.image_url || ''} alt={zoomed}
              className="w-full max-h-64 object-contain invert opacity-90" />
            <span className="text-ink font-semibold">{zoomed}</span>
            <div className="flex gap-3 mt-2">
              <button onClick={(e) => { e.stopPropagation(); toggle(zoomed); setZoomed(null); }}
                className={`px-4 py-2 rounded-lg text-sm font-semibold ${
                  selected.has(zoomed) ? 'bg-border/30 text-muted' : 'bg-gold text-black'
                }`}>
                {selected.has(zoomed) ? 'Deselect' : 'Select this shape'}
              </button>
              <button onClick={() => setZoomed(null)}
                className="px-4 py-2 rounded-lg text-sm text-muted border border-border">
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-5 gap-3 w-full max-w-2xl">
        {formsWithImages.map((s) => (
          <button key={s.slug} onClick={() => toggle(s.name)}
            onContextMenu={(e) => { e.preventDefault(); setZoomed(s.name); }}
            className={`flex flex-col items-center gap-1.5 p-3 rounded-xl border-2 transition-colors ${
              selected.has(s.name) ? 'border-gold bg-gold/10' : 'border-border bg-card hover:border-border/70'
            }`}>
            <div className="w-24 h-16 flex items-center justify-center">
              <img src={s.image_url!} alt={s.name} className="max-w-full max-h-full object-contain invert opacity-80" />
            </div>
            <span className="text-xs text-ink font-medium leading-tight text-center">{s.name}</span>
          </button>
        ))}
      </div>

      <button onClick={() => onAnswer([...selected])} disabled={loading || selected.size === 0}
        className="mt-2 px-6 py-2.5 rounded-lg bg-gold text-black font-semibold text-sm hover:bg-gold/90 disabled:opacity-40 transition-colors">
        Continue with {selected.size} selected
      </button>
      <SkipButton onClick={onSkip} loading={loading} />
    </div>
  );
}

function CandidateGrid({
  candidates, onSelect,
}: {
  candidates: WizardCandidate[];
  onSelect: (candidate: WizardCandidate) => void;
}) {
  return (
    <div className="py-6 px-6">
      <h2 className="text-lg font-semibold text-ink text-center mb-1">
        {candidates.length === 1 ? 'We found your knife!' : `${candidates.length} candidates remain`}
      </h2>
      <p className="text-xs text-muted text-center mb-6">
        {candidates.length === 1 ? 'Is this the right one?' : 'Select the knife that matches yours'}
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 max-w-4xl mx-auto">
        {candidates.map((c) => {
          const imgSrc = c.best_colorway_id
            ? `/api/v2/colorways/${c.best_colorway_id}/image`
            : c.has_image ? `/api/v2/models/${c.model_id}/image` : null;

          return (
            <button key={c.model_id} onClick={() => onSelect(c)}
              className="flex flex-col border-2 border-border bg-card rounded-xl overflow-hidden hover:border-gold/50 hover:bg-gold/5 transition-colors text-left">
              <div className="aspect-[4/3] bg-white flex items-center justify-center overflow-hidden">
                {imgSrc ? (
                  <img src={imgSrc} alt={c.name} className="w-full h-full object-contain p-2" />
                ) : (
                  <div className="text-muted/30 text-2xl">?</div>
                )}
              </div>
              <div className="px-3 py-2.5">
                <div className="text-sm font-semibold text-ink leading-tight">{c.name}</div>
                <div className="flex flex-wrap gap-x-2 gap-y-0.5 mt-1">
                  {c.family && <span className="text-xs text-muted">{c.family}</span>}
                  {c.form && <span className="text-xs text-muted">{c.form}</span>}
                  {c.blade_length && <span className="text-xs text-muted">{c.blade_length}&Prime;</span>}
                  {c.handle_type && <span className="text-xs text-muted">{c.handle_type}</span>}
                </div>
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function CandidateDetail({
  candidate, onBack, onConfirm,
}: {
  candidate: WizardCandidate;
  onBack: () => void;
  onConfirm: () => void;
}) {
  const imgSrc = candidate.best_colorway_id
    ? `/api/v2/colorways/${candidate.best_colorway_id}/image`
    : candidate.has_image ? `/api/v2/models/${candidate.model_id}/image` : null;

  return (
    <div className="flex flex-col h-full overflow-y-auto">
      <div className="flex items-center gap-3 px-6 py-3 border-b border-border flex-shrink-0">
        <button onClick={onBack} className="p-1.5 rounded-lg border border-border text-muted hover:text-ink transition-colors">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 18 9 12 15 6" />
          </svg>
        </button>
        <h3 className="text-ink text-base font-bold truncate flex-1">{candidate.name}</h3>
      </div>

      <div className="flex-1 p-4">
        <div className="max-w-md mx-auto">
          <div className="aspect-[4/3] bg-white rounded-xl overflow-hidden">
            {imgSrc ? (
              <img src={imgSrc} alt={candidate.name} className="w-full h-full object-contain p-2" />
            ) : (
              <div className="w-full h-full flex items-center justify-center text-muted/30">No image</div>
            )}
          </div>
        </div>

        <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-3 max-w-3xl mx-auto">
          {candidate.family && <div className="text-xs"><span className="text-muted">Family:</span> <span className="text-ink">{candidate.family}</span></div>}
          {candidate.form && <div className="text-xs"><span className="text-muted">Form:</span> <span className="text-ink">{candidate.form}</span></div>}
          {candidate.handle_type && <div className="text-xs"><span className="text-muted">Handle:</span> <span className="text-ink">{candidate.handle_type}</span></div>}
          {candidate.blade_length && <div className="text-xs"><span className="text-muted">Length:</span> <span className="text-ink">{candidate.blade_length}&Prime;</span></div>}
          {candidate.blade_steel && <div className="text-xs"><span className="text-muted">Steel:</span> <span className="text-ink">{candidate.blade_steel}</span></div>}
          {candidate.blade_finish && <div className="text-xs"><span className="text-muted">Finish:</span> <span className="text-ink">{candidate.blade_finish}</span></div>}
          {candidate.msrp && <div className="text-xs"><span className="text-muted">MSRP:</span> <span className="text-ink">${candidate.msrp}</span></div>}
        </div>

        <div className="flex justify-center gap-3 mt-6">
          <button onClick={onBack}
            className="px-4 py-2.5 rounded-lg border border-border text-sm text-muted hover:text-ink transition-colors">
            Not this one
          </button>
          <button onClick={onConfirm}
            className="px-6 py-2.5 rounded-lg bg-gold text-black font-semibold text-sm hover:bg-gold/90 transition-colors">
            This is my knife — Add to Collection
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

const SIDEBAR_KEY = 'mkc_sidebar_collapsed';

export default function Identify() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(
    () => localStorage.getItem(SIDEBAR_KEY) === 'true',
  );

  useEffect(() => {
    const handler = (e: Event) => {
      const ce = e as CustomEvent;
      setSidebarCollapsed(ce.detail.collapsed);
    };
    window.addEventListener('mkc-sidebar-toggle', handler);
    return () => window.removeEventListener('mkc-sidebar-toggle', handler);
  }, []);

  const marginClass = sidebarCollapsed ? 'md:ml-16' : 'md:ml-56';

  // Wizard state
  const [phase, setPhase] = useState<'start' | 'questions' | 'candidates' | 'detail'>('start');
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [totalModels, setTotalModels] = useState(87);
  const [remainingModels, setRemainingModels] = useState(87);
  const [remainingFamilies, setRemainingFamilies] = useState(42);
  const [currentQuestion, setCurrentQuestion] = useState<WizardQuestion | null>(null);
  const [answeredQuestions, setAnsweredQuestions] = useState<AnsweredQuestion[]>([]);
  const [candidates, setCandidates] = useState<WizardCandidate[]>([]);
  const [selectedCandidate, setSelectedCandidate] = useState<WizardCandidate | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [silhouettes, setSilhouettes] = useState<BladeFormSilhouette[]>([]);

  useEffect(() => {
    fetchBladeFormSilhouettes().then(setSilhouettes).catch(() => {});
  }, []);

  // Auto-start wizard on mount
  const handleStart = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const result = await wizardStart();
      setSessionId(result.session_id);
      setTotalModels(result.total_models);
      setRemainingModels(result.remaining_models);
      setRemainingFamilies(result.remaining_families);

      if (result.done) {
        setCandidates(result.candidates || []);
        setPhase('candidates');
      } else {
        setCurrentQuestion(result.next_question);
        setPhase('questions');
      }
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    handleStart();
  }, [handleStart]);

  // Answer a question
  const handleAnswer = useCallback(async (answer: any) => {
    if (!sessionId || !currentQuestion) return;
    setLoading(true);
    setError(null);
    try {
      let answerLabel = String(answer);
      if (currentQuestion.type === 'boolean') {
        answerLabel = answer === null ? 'Skipped' : answer ? 'Yes' : 'No';
      } else if (currentQuestion.options) {
        const opt = currentQuestion.options.find((o) => o.value === answer);
        if (opt) answerLabel = opt.label;
      } else if (Array.isArray(answer)) {
        answerLabel = answer.join(', ');
      }
      if (answer === null) answerLabel = 'Skipped';

      setAnsweredQuestions((prev) => [
        ...prev,
        { key: currentQuestion.key, display_text: currentQuestion.display_text, answer, answerLabel },
      ]);

      const result = await wizardAnswer(sessionId, currentQuestion.key, answer);
      setRemainingModels(result.remaining_models);
      setRemainingFamilies(result.remaining_families);

      if (result.done) {
        setCandidates(result.candidates || []);
        setCurrentQuestion(null);
        setPhase('candidates');
      } else {
        setCurrentQuestion(result.next_question);
      }
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [sessionId, currentQuestion]);

  const handleSkip = useCallback(() => { handleAnswer(null); }, [handleAnswer]);

  const handleBack = useCallback(async () => {
    if (!sessionId) return;
    setLoading(true);
    try {
      const result = await wizardBack(sessionId);
      setRemainingModels(result.remaining_models);
      setRemainingFamilies(result.remaining_families);
      setCurrentQuestion(result.next_question);
      setAnsweredQuestions((prev) => prev.slice(0, -1));
      if (phase === 'candidates') setPhase('questions');
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [sessionId, phase]);

  const handleReset = useCallback(() => {
    setPhase('start');
    setSessionId(null);
    setCurrentQuestion(null);
    setAnsweredQuestions([]);
    setCandidates([]);
    setSelectedCandidate(null);
    setError(null);
    setRemainingModels(87);
    setRemainingFamilies(42);
    // Re-start
    setTimeout(() => { handleStart(); }, 0);
  }, [handleStart]);

  const handleConfirm = useCallback(() => {
    if (!selectedCandidate) return;
    window.location.href = `/collection?add=${selectedCandidate.model_id}${
      selectedCandidate.best_colorway_id ? `&colorway=${selectedCandidate.best_colorway_id}` : ''
    }`;
  }, [selectedCandidate]);

  // Render question
  const renderQuestion = () => {
    if (!currentQuestion) return null;
    if (currentQuestion.type === 'boolean') {
      return <BooleanQuestion question={currentQuestion} onAnswer={handleAnswer} onSkip={handleSkip} loading={loading} />;
    }
    if (currentQuestion.key === 'blade_form' && currentQuestion.visual_aid === 'blade_form_silhouettes') {
      return <BladeFormQuestion question={currentQuestion} silhouettes={silhouettes} onAnswer={handleAnswer} onSkip={handleSkip} loading={loading} />;
    }
    return <ChoiceQuestion question={currentQuestion} onAnswer={handleAnswer} onSkip={handleSkip} loading={loading} />;
  };

  const renderContent = () => {
    if (phase === 'start') {
      return (
        <div className="flex items-center justify-center py-16">
          <div className="text-muted text-sm">Starting wizard...</div>
        </div>
      );
    }

    if (phase === 'detail' && selectedCandidate) {
      return (
        <CandidateDetail
          candidate={selectedCandidate}
          onBack={() => { setSelectedCandidate(null); setPhase('candidates'); }}
          onConfirm={handleConfirm}
        />
      );
    }

    if (phase === 'candidates') {
      return <CandidateGrid candidates={candidates} onSelect={(c) => { setSelectedCandidate(c); setPhase('detail'); }} />;
    }

    return renderQuestion();
  };

  return (
    <div className="h-screen-safe bg-surface text-ink">
      <Sidebar />

      <main className={`${marginClass} transition-[margin] duration-200 flex flex-col h-screen-safe overflow-hidden`}>
        <header className="flex items-center justify-between pl-14 md:pl-6 pr-6 py-3 border-b border-border flex-shrink-0">
          <div className="flex items-center gap-3 min-w-0">
            <h1 className="text-lg font-bold truncate">Identify</h1>
          </div>
          <div className="flex items-center gap-2">
            {phase !== 'start' && (
              <>
                {answeredQuestions.length > 0 && phase === 'questions' && (
                  <button onClick={handleBack} disabled={loading}
                    className="px-3 py-1.5 rounded-lg border border-border text-xs text-muted hover:text-ink transition-colors disabled:opacity-40">
                    Back
                  </button>
                )}
                <button onClick={handleReset}
                  className="px-3 py-1.5 rounded-lg border border-border text-xs text-muted hover:text-ink transition-colors">
                  Start Over
                </button>
              </>
            )}
          </div>
        </header>

        {phase === 'questions' && (
          <ProgressBar totalModels={totalModels} remainingModels={remainingModels} remainingFamilies={remainingFamilies} />
        )}

        {answeredQuestions.length > 0 && phase === 'questions' && (
          <AnsweredList answered={answeredQuestions} />
        )}

        {error && (
          <div className="mx-6 mt-2 px-4 py-2 rounded-lg bg-red-900/20 border border-red-800/30 text-red-300 text-sm">
            {error}
          </div>
        )}

        <div className="flex-1 overflow-y-auto">
          {renderContent()}
        </div>
      </main>
    </div>
  );
}
