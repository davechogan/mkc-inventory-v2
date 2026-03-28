import type { FamilyStat } from '../types';

interface FamilyChipsProps {
  families: FamilyStat[];
  activeFamily: string;
  onSelect: (f: string) => void;
}

export function FamilyChips({ families, activeFamily, onSelect }: FamilyChipsProps) {
  const handleClick = (family: string) => {
    onSelect(activeFamily === family ? '' : family);
  };

  return (
    <div className="flex items-center gap-2 overflow-x-auto scrollbar-hide py-1">
      {families.map((fs) => {
        const isActive = activeFamily === fs.family;
        return (
          <button
            key={fs.family}
            onClick={() => handleClick(fs.family)}
            className={`flex items-center gap-1.5 flex-shrink-0 px-3 py-1.5 rounded-full border text-sm font-medium transition-all duration-150 ${
              isActive
                ? 'border-gold bg-gold/10 text-ink'
                : 'border-border text-muted hover:border-border/80 hover:text-ink'
            }`}
          >
            <span>{fs.family}</span>
            <span
              className={`text-xs font-bold ${
                isActive ? 'text-gold-bright' : 'text-muted'
              }`}
            >
              {fs.total_quantity}
            </span>
          </button>
        );
      })}
    </div>
  );
}
