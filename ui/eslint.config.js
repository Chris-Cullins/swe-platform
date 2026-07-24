import js from '@eslint/js'
import globals from 'globals'
import hooks from 'eslint-plugin-react-hooks'
import refresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'

export default tseslint.config(
  { ignores: ['dist'] }, js.configs.recommended, ...tseslint.configs.recommended,
  { files: ['**/*.{ts,tsx}'], languageOptions: { globals: { ...globals.browser, ...globals.node } }, plugins: { 'react-hooks': hooks, 'react-refresh': refresh }, rules: { 'react-hooks/rules-of-hooks': 'error', 'react-hooks/exhaustive-deps': 'warn', 'react-refresh/only-export-components': ['warn', { allowConstantExport: true }] } },
)
