import { useState, useEffect, useCallback, useRef } from 'react';
import { Sidebar } from '../components/Sidebar';

// ── Types ─────────────────────────────────────────────────────────────────────

interface WizardQuestion {
  key: string;
  display_text: string;
  type: 'boolean' | 'single_choice' | 'multi_choice';
  options: { value: any; label: string; color?: string }[] | null;
  visual_aid: string | null;
  vision_suggestion: any;
  vision_reliability: number;
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

interface AutoGate {
  question: string;
  display_text: string;
  vision_answer: boolean;
  eliminated: number;
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

async function wizardStart(imageFile: File | null): Promise<any> {
  const fd = new FormData();
  if (imageFile) fd.append('image', imageFile);
  const res = await fetch('/api/v2/identify/wizard/start', { method: 'POST', body: fd });
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

// ── Constants ─────────────────────────────────────────────────────────────────

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

function AutoGateChips({ gates }: { gates: AutoGate[] }) {
  if (gates.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-2 px-6 py-2">
      {gates.map((g) => (
        <span
          key={g.question}
          className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs bg-border/20 text-muted"
        >
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="20 6 9 17 4 12" />
          </svg>
          {g.vision_answer ? g.display_text.replace('?', '') : `Not: ${g.display_text.replace('?', '').replace('Is this a ', '').replace('Is the ', '')}`}
        </span>
      ))}
    </div>
  );
}

function AnsweredList({ answered }: { answered: AnsweredQuestion[] }) {
  if (answered.length === 0) return null;
  return (
    <div className="px-6 py-2 border-b border-border/50">
      <div className="flex flex-wrap gap-x-4 gap-y-1">
        {answered.map((a) => (
          <span key={a.key} className="text-xs text-muted">
            <span className="text-ink/60">{a.display_text.split('?')[0].replace('Is the handle wrapped in paracord or cord', 'Paracord').replace('Is this a kitchen/culinary knife or a field/hunting knife', 'Type').replace('Is there a finger ring at the front of the handle', 'Finger ring').replace('What color is the blade', 'Blade').replace('What is the primary handle color', 'Handle color').replace('What is the handle material', 'Material').replace('Approximately how long is the blade', 'Length').replace('Which blade shape(s) match your knife', 'Form')}:</span>{' '}
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
  question,
  onAnswer,
  onSkip,
  loading,
}: {
  question: WizardQuestion;
  onAnswer: (answer: boolean) => void;
  onSkip: () => void;
  loading: boolean;
}) {
  const suggestion = question.vision_suggestion;
  const hasSuggestion = suggestion !== null && suggestion !== undefined;

  return (
    <div className="flex flex-col items-center gap-6 py-6 px-6">
      <h2 className="text-lg font-semibold text-ink text-center">{question.display_text}</h2>
      {hasSuggestion && question.vision_reliability >= 0.8 && (
        <div className="text-xs text-muted/70 flex items-center gap-1.5">
          <span className="w-2 h-2 rounded-full bg-gold/60" />
          AI suggests: <span className="text-gold font-medium">{suggestion ? 'Yes' : 'No'}</span>
        </div>
      )}
      <div className="flex gap-4">
        <button
          onClick={() => onAnswer(true)}
          disabled={loading}
          className={`px-8 py-4 rounded-xl text-base font-semibold transition-colors border-2 ${
            hasSuggestion && suggestion === true
              ? 'border-gold bg-gold/10 text-gold hover:bg-gold/20'
              : 'border-border bg-card text-ink hover:border-border/70 hover:bg-border/10'
          } disabled:opacity-40`}
        >
          Yes
        </button>
        <button
          onClick={() => onAnswer(false)}
          disabled={loading}
          className={`px-8 py-4 rounded-xl text-base font-semibold transition-colors border-2 ${
            hasSuggestion && suggestion === false
              ? 'border-gold bg-gold/10 text-gold hover:bg-gold/20'
              : 'border-border bg-card text-ink hover:border-border/70 hover:bg-border/10'
          } disabled:opacity-40`}
        >
          No
        </button>
      </div>
      <SkipButton onClick={onSkip} loading={loading} />
    </div>
  );
}

function ChoiceQuestion({
  question,
  onAnswer,
  onSkip,
  loading,
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
            <button
              key={String(opt.value)}
              onClick={() => onAnswer(opt.value)}
              disabled={loading}
              className="flex flex-col items-center gap-1.5 px-3 py-2 rounded-xl border border-border bg-card hover:border-gold/50 hover:bg-gold/5 transition-colors disabled:opacity-40"
            >
              {opt.color && (
                <div
                  className="w-8 h-8 rounded-full border border-border/50"
                  style={{ backgroundColor: opt.color }}
                />
              )}
              <span className="text-xs text-ink font-medium">{opt.label}</span>
            </button>
          ))}
        </div>
      ) : (
        <div className="flex flex-col gap-2 w-full max-w-sm">
          {options.map((opt) => (
            <button
              key={String(opt.value)}
              onClick={() => onAnswer(opt.value)}
              disabled={loading}
              className="w-full px-4 py-3 rounded-xl border border-border bg-card text-left text-sm text-ink font-medium hover:border-gold/50 hover:bg-gold/5 transition-colors disabled:opacity-40"
            >
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
  question,
  silhouettes,
  onAnswer,
  onSkip,
  loading,
}: {
  question: WizardQuestion;
  silhouettes: BladeFormSilhouette[];
  onAnswer: (answer: string[]) => void;
  onSkip: () => void;
  loading: boolean;
}) {
  const [selected, setSelected] = useState<Set<string>>(new Set());

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
      <p className="text-xs text-muted">Select one or more shapes that look like your knife</p>

      <div className="grid grid-cols-3 sm:grid-cols-4 md:grid-cols-5 gap-3 w-full max-w-2xl">
        {formsWithImages.map((s) => (
          <button
            key={s.slug}
            onClick={() => toggle(s.name)}
            className={`flex flex-col items-center gap-1 p-2 rounded-xl border-2 transition-colors ${
              selected.has(s.name)
                ? 'border-gold bg-gold/10'
                : 'border-border bg-card hover:border-border/70'
            }`}
          >
            <div className="w-16 h-12 flex items-center justify-center">
              <img
                src={s.image_url!}
                alt={s.name}
                className="max-w-full max-h-full object-contain invert opacity-80"
              />
            </div>
            <span className="text-[10px] text-ink font-medium leading-tight text-center">
              {s.name}
            </span>
          </button>
        ))}
      </div>

      <button
        onClick={() => onAnswer([...selected])}
        disabled={loading || selected.size === 0}
        className="mt-2 px-6 py-2.5 rounded-lg bg-gold text-black font-semibold text-sm hover:bg-gold/90 disabled:opacity-40 transition-colors"
      >
        Continue with {selected.size} selected
      </button>
      <SkipButton onClick={onSkip} loading={loading} />
    </div>
  );
}

function CandidateGrid({
  candidates,
  onSelect,
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
        {candidates.length === 1
          ? 'Is this the right one?'
          : 'Select the knife that matches yours'}
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 max-w-4xl mx-auto">
        {candidates.map((c) => {
          const imgSrc = c.best_colorway_id
            ? `/api/v2/colorways/${c.best_colorway_id}/image`
            : c.has_image
              ? `/api/v2/models/${c.model_id}/image`
              : null;

          return (
            <button
              key={c.model_id}
              onClick={() => onSelect(c)}
              className="flex flex-col border border-border bg-card rounded-xl overflow-hidden hover:border-gold/50 hover:bg-gold/5 transition-colors text-left"
            >
              <div className="aspect-[4/3] bg-border/10 flex items-center justify-center overflow-hidden">
                {imgSrc ? (
                  <img src={imgSrc} alt={c.name} className="w-full h-full object-cover" />
                ) : (
                  <div className="text-muted/30 text-2xl">?</div>
                )}
              </div>
              <div className="px-3 py-2.5">
                <div className="text-sm font-semibold text-ink leading-tight">{c.name}</div>
                <div className="flex flex-wrap gap-x-2 gap-y-0.5 mt-1">
                  {c.family && <span className="text-xs text-muted">{c.family}</span>}
                  {c.form && <span className="text-xs text-muted">{c.form}</span>}
                  {c.blade_length && (
                    <span className="text-xs text-muted">{c.blade_length}&Prime;</span>
                  )}
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
  candidate,
  userImage,
  onBack,
  onConfirm,
}: {
  candidate: WizardCandidate;
  userImage: string | null;
  onBack: () => void;
  onConfirm: () => void;
}) {
  const imgSrc = candidate.best_colorway_id
    ? `/api/v2/colorways/${candidate.best_colorway_id}/image`
    : candidate.has_image
      ? `/api/v2/models/${candidate.model_id}/image`
      : null;

  return (
    <div className="flex flex-col h-full overflow-y-auto">
      <div className="flex items-center gap-3 px-6 py-3 border-b border-border flex-shrink-0">
        <button
          onClick={onBack}
          className="p-1.5 rounded-lg border border-border text-muted hover:text-ink transition-colors"
        >
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="15 18 9 12 15 6" />
          </svg>
        </button>
        <h3 className="text-ink text-base font-bold truncate flex-1">{candidate.name}</h3>
      </div>

      <div className="flex-1 p-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-3xl mx-auto">
          {userImage && (
            <div className="flex flex-col gap-1">
              <span className="text-xs text-muted font-medium">Your Knife</span>
              <div className="aspect-[4/3] bg-border/10 rounded-xl overflow-hidden">
                <img src={userImage} alt="Your knife" className="w-full h-full object-contain" />
              </div>
            </div>
          )}
          <div className="flex flex-col gap-1">
            <span className="text-xs text-muted font-medium">Reference</span>
            <div className="aspect-[4/3] bg-border/10 rounded-xl overflow-hidden">
              {imgSrc ? (
                <img src={imgSrc} alt={candidate.name} className="w-full h-full object-contain" />
              ) : (
                <div className="w-full h-full flex items-center justify-center text-muted/30">No image</div>
              )}
            </div>
          </div>
        </div>

        <div className="mt-4 grid grid-cols-2 md:grid-cols-4 gap-3 max-w-3xl mx-auto">
          {candidate.family && (
            <div className="text-xs"><span className="text-muted">Family:</span> <span className="text-ink">{candidate.family}</span></div>
          )}
          {candidate.form && (
            <div className="text-xs"><span className="text-muted">Form:</span> <span className="text-ink">{candidate.form}</span></div>
          )}
          {candidate.handle_type && (
            <div className="text-xs"><span className="text-muted">Handle:</span> <span className="text-ink">{candidate.handle_type}</span></div>
          )}
          {candidate.blade_length && (
            <div className="text-xs"><span className="text-muted">Length:</span> <span className="text-ink">{candidate.blade_length}&Prime;</span></div>
          )}
          {candidate.blade_steel && (
            <div className="text-xs"><span className="text-muted">Steel:</span> <span className="text-ink">{candidate.blade_steel}</span></div>
          )}
          {candidate.blade_finish && (
            <div className="text-xs"><span className="text-muted">Finish:</span> <span className="text-ink">{candidate.blade_finish}</span></div>
          )}
          {candidate.msrp && (
            <div className="text-xs"><span className="text-muted">MSRP:</span> <span className="text-ink">${candidate.msrp}</span></div>
          )}
        </div>

        <div className="flex justify-center gap-3 mt-6">
          <button
            onClick={onBack}
            className="px-4 py-2.5 rounded-lg border border-border text-sm text-muted hover:text-ink transition-colors"
          >
            Not this one
          </button>
          <button
            onClick={onConfirm}
            className="px-6 py-2.5 rounded-lg bg-gold text-black font-semibold text-sm hover:bg-gold/90 transition-colors"
          >
            This is my knife — Add to Collection
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Upload Step ──────────────────────────────────────────────────────────────

function UploadStep({
  onStart,
  loading,
}: {
  onStart: (file: File | null) => void;
  loading: boolean;
}) {
  const [imageFile, setImageFile] = useState<File | null>(null);
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFile = useCallback((file: File) => {
    setImageFile(file);
    const reader = new FileReader();
    reader.onload = () => setImagePreview(reader.result as string);
    reader.readAsDataURL(file);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      const file = e.dataTransfer.files[0];
      if (file?.type.startsWith('image/')) handleFile(file);
    },
    [handleFile],
  );

  return (
    <div className="flex flex-col items-center gap-6 py-8 px-6 max-w-lg mx-auto">
      <h2 className="text-xl font-bold text-ink">Identify Your Knife</h2>
      <p className="text-sm text-muted text-center">
        Upload a photo of your MKC knife and we'll walk you through a few questions to identify it.
      </p>

      <div
        onDragOver={(e) => e.preventDefault()}
        onDrop={handleDrop}
        onClick={() => fileInputRef.current?.click()}
        className={`w-full aspect-[4/3] rounded-xl border-2 border-dashed cursor-pointer transition-colors flex items-center justify-center overflow-hidden ${
          imagePreview
            ? 'border-gold/40 bg-gold/5'
            : 'border-border hover:border-border/70 bg-card'
        }`}
      >
        {imagePreview ? (
          <img src={imagePreview} alt="Preview" className="w-full h-full object-contain" />
        ) : (
          <div className="flex flex-col items-center gap-2 text-muted">
            <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="opacity-40">
              <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
              <circle cx="8.5" cy="8.5" r="1.5" />
              <polyline points="21 15 16 10 5 21" />
            </svg>
            <span className="text-sm">Drop a photo here or click to browse</span>
          </div>
        )}
        <input
          ref={fileInputRef}
          type="file"
          accept="image/*"
          className="hidden"
          onChange={(e) => e.target.files?.[0] && handleFile(e.target.files[0])}
        />
      </div>

      <div className="flex gap-3">
        <button
          onClick={() => onStart(imageFile)}
          disabled={loading}
          className="px-6 py-2.5 rounded-lg bg-gold text-black font-semibold text-sm hover:bg-gold/90 disabled:opacity-40 transition-colors"
        >
          {loading ? 'Analyzing...' : imageFile ? 'Start Identification' : 'Start Without Photo'}
        </button>
      </div>

      {!imageFile && (
        <p className="text-xs text-muted/60 text-center">
          A photo helps the AI suggest answers, but you can identify by answering questions alone.
        </p>
      )}
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────

export default function Identify() {
  // Wizard state
  const [phase, setPhase] = useState<'upload' | 'questions' | 'candidates' | 'detail'>('upload');
  const [sessionId, setSessionId] = useState<string | null>(null);
  const [imagePreview, setImagePreview] = useState<string | null>(null);
  const [totalModels, setTotalModels] = useState(87);
  const [remainingModels, setRemainingModels] = useState(87);
  const [remainingFamilies, setRemainingFamilies] = useState(42);
  const [autoGates, setAutoGates] = useState<AutoGate[]>([]);
  const [currentQuestion, setCurrentQuestion] = useState<WizardQuestion | null>(null);
  const [answeredQuestions, setAnsweredQuestions] = useState<AnsweredQuestion[]>([]);
  const [candidates, setCandidates] = useState<WizardCandidate[]>([]);
  const [selectedCandidate, setSelectedCandidate] = useState<WizardCandidate | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [silhouettes, setSilhouettes] = useState<BladeFormSilhouette[]>([]);

  // Load silhouettes once
  useEffect(() => {
    fetchBladeFormSilhouettes().then(setSilhouettes).catch(() => {});
  }, []);

  // Start wizard
  const handleStart = useCallback(async (imageFile: File | null) => {
    setLoading(true);
    setError(null);
    try {
      // Save preview for later
      if (imageFile) {
        const reader = new FileReader();
        reader.onload = () => setImagePreview(reader.result as string);
        reader.readAsDataURL(imageFile);
      }

      const result = await wizardStart(imageFile);
      setSessionId(result.session_id);
      setTotalModels(result.total_models);
      setRemainingModels(result.remaining_models);
      setRemainingFamilies(result.remaining_families);
      setAutoGates(result.auto_gates || []);

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

  // Answer a question
  const handleAnswer = useCallback(async (answer: any) => {
    if (!sessionId || !currentQuestion) return;
    setLoading(true);
    setError(null);
    try {
      // Build display label
      let answerLabel = String(answer);
      if (currentQuestion.type === 'boolean') {
        answerLabel = answer ? 'Yes' : 'No';
      } else if (currentQuestion.options) {
        const opt = currentQuestion.options.find((o) => o.value === answer);
        if (opt) answerLabel = opt.label;
      } else if (Array.isArray(answer)) {
        answerLabel = answer.join(', ');
      }

      setAnsweredQuestions((prev) => [
        ...prev,
        {
          key: currentQuestion.key,
          display_text: currentQuestion.display_text,
          answer,
          answerLabel,
        },
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

  // Go back
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

  // Reset
  const handleReset = useCallback(() => {
    setPhase('upload');
    setSessionId(null);
    setImagePreview(null);
    setAutoGates([]);
    setCurrentQuestion(null);
    setAnsweredQuestions([]);
    setCandidates([]);
    setSelectedCandidate(null);
    setError(null);
    setRemainingModels(87);
    setRemainingFamilies(42);
  }, []);

  // Select candidate
  const handleSelectCandidate = useCallback((c: WizardCandidate) => {
    setSelectedCandidate(c);
    setPhase('detail');
  }, []);

  // Confirm selection (add to inventory)
  const handleConfirm = useCallback(() => {
    if (!selectedCandidate) return;
    // Navigate to collection page with pre-selected model
    // For now, open the inventory add page
    window.location.href = `/collection?add=${selectedCandidate.model_id}${
      selectedCandidate.best_colorway_id ? `&colorway=${selectedCandidate.best_colorway_id}` : ''
    }`;
  }, [selectedCandidate]);

  // Handle skip (I don't know)
  const handleSkip = useCallback(() => {
    handleAnswer(null);
  }, [handleAnswer]);

  // Render the question component
  const renderQuestion = () => {
    if (!currentQuestion) return null;

    if (currentQuestion.type === 'boolean') {
      return (
        <BooleanQuestion
          question={currentQuestion}
          onAnswer={handleAnswer}
          onSkip={handleSkip}
          loading={loading}
        />
      );
    }

    if (currentQuestion.key === 'blade_form' && currentQuestion.visual_aid === 'blade_form_silhouettes') {
      return (
        <BladeFormQuestion
          question={currentQuestion}
          silhouettes={silhouettes}
          onAnswer={handleAnswer}
          onSkip={handleSkip}
          loading={loading}
        />
      );
    }

    return (
      <ChoiceQuestion
        question={currentQuestion}
        onAnswer={handleAnswer}
        onSkip={handleSkip}
        loading={loading}
      />
    );
  };

  // Render current step
  const renderContent = () => {
    if (phase === 'upload') {
      return <UploadStep onStart={handleStart} loading={loading} />;
    }

    if (phase === 'detail' && selectedCandidate) {
      return (
        <CandidateDetail
          candidate={selectedCandidate}
          userImage={imagePreview}
          onBack={() => { setSelectedCandidate(null); setPhase('candidates'); }}
          onConfirm={handleConfirm}
        />
      );
    }

    if (phase === 'candidates') {
      return (
        <CandidateGrid
          candidates={candidates}
          onSelect={handleSelectCandidate}
        />
      );
    }

    // Questions phase — show image alongside question
    return (
      <div className="flex flex-col md:flex-row gap-4 p-4 max-w-4xl mx-auto w-full">
        {/* Persistent uploaded image */}
        {imagePreview && (
          <div className="md:w-1/3 flex-shrink-0">
            <div className="sticky top-4">
              <span className="text-xs text-muted font-medium mb-1 block">Your knife</span>
              <div className="rounded-xl overflow-hidden border border-border bg-card">
                <img src={imagePreview} alt="Your knife" className="w-full object-contain max-h-64 md:max-h-80" />
              </div>
            </div>
          </div>
        )}
        {/* Question area */}
        <div className={imagePreview ? 'md:flex-1' : 'w-full'}>
          {renderQuestion()}
        </div>
      </div>
    );
  };

  return (
    <div className="flex h-screen-safe bg-surface text-ink">
      <Sidebar />

      <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Header */}
        <header className="flex items-center justify-between px-6 py-3 border-b border-border flex-shrink-0">
          <div className="flex items-center gap-3">
            <h1 className="text-lg font-bold">Identify</h1>
          </div>
          <div className="flex items-center gap-2">
            {phase !== 'upload' && (
              <>
                {answeredQuestions.length > 0 && phase === 'questions' && (
                  <button
                    onClick={handleBack}
                    disabled={loading}
                    className="px-3 py-1.5 rounded-lg border border-border text-xs text-muted hover:text-ink transition-colors disabled:opacity-40"
                  >
                    Back
                  </button>
                )}
                <button
                  onClick={handleReset}
                  className="px-3 py-1.5 rounded-lg border border-border text-xs text-muted hover:text-ink transition-colors"
                >
                  Start Over
                </button>
              </>
            )}
          </div>
        </header>

        {/* Progress bar */}
        {phase !== 'upload' && (
          <ProgressBar
            totalModels={totalModels}
            remainingModels={remainingModels}
            remainingFamilies={remainingFamilies}
          />
        )}

        {/* Auto-gate chips */}
        {autoGates.length > 0 && phase === 'questions' && (
          <AutoGateChips gates={autoGates} />
        )}

        {/* Answered summary */}
        {answeredQuestions.length > 0 && phase === 'questions' && (
          <AnsweredList answered={answeredQuestions} />
        )}

        {/* Error */}
        {error && (
          <div className="mx-6 mt-2 px-4 py-2 rounded-lg bg-red-900/20 border border-red-800/30 text-red-300 text-sm">
            {error}
          </div>
        )}

        {/* Main content area */}
        <div className="flex-1 overflow-y-auto">
          {renderContent()}
        </div>
      </main>
    </div>
  );
}
