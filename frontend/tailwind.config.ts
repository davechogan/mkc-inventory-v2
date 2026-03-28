import type { Config } from 'tailwindcss';

const config: Config = {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        surface: '#09090c',
        sidebar: '#060709',
        card: '#0f1114',
        border: '#1d2329',
        ink: '#f0f2f4',
        muted: '#7a8899',
        gold: '#c8921a',
        'gold-bright': '#e8a820',
      },
      fontFamily: {
        sans: ['Inter', 'system-ui', 'sans-serif'],
      },
    },
  },
  plugins: [],
};

export default config;
