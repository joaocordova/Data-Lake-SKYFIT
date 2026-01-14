-- ============================================================================
-- ADICIONAR COLUNAS FALTANTES - EVO MEMBERS
-- ============================================================================
-- Execute este script para adicionar as novas colunas à tabela existente
-- ============================================================================

-- Adiciona colunas de parceiros (Totalpass, Gurupass)
DO $$
BEGIN
    -- code_totalpass
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'core' AND table_name = 'evo_members' AND column_name = 'code_totalpass') THEN
        ALTER TABLE core.evo_members ADD COLUMN code_totalpass TEXT;
        RAISE NOTICE 'Coluna code_totalpass adicionada';
    END IF;
    
    -- user_id_gurupass
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'core' AND table_name = 'evo_members' AND column_name = 'user_id_gurupass') THEN
        ALTER TABLE core.evo_members ADD COLUMN user_id_gurupass TEXT;
        RAISE NOTICE 'Coluna user_id_gurupass adicionada';
    END IF;
    
    -- client_with_promotional_restriction
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'core' AND table_name = 'evo_members' AND column_name = 'client_with_promotional_restriction') THEN
        ALTER TABLE core.evo_members ADD COLUMN client_with_promotional_restriction BOOLEAN DEFAULT FALSE;
        RAISE NOTICE 'Coluna client_with_promotional_restriction adicionada';
    END IF;
    
    -- register_name
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'core' AND table_name = 'evo_members' AND column_name = 'register_name') THEN
        ALTER TABLE core.evo_members ADD COLUMN register_name TEXT;
        RAISE NOTICE 'Coluna register_name adicionada';
    END IF;
    
    -- register_last_name
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'core' AND table_name = 'evo_members' AND column_name = 'register_last_name') THEN
        ALTER TABLE core.evo_members ADD COLUMN register_last_name TEXT;
        RAISE NOTICE 'Coluna register_last_name adicionada';
    END IF;
    
    -- use_preferred_name
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_schema = 'core' AND table_name = 'evo_members' AND column_name = 'use_preferred_name') THEN
        ALTER TABLE core.evo_members ADD COLUMN use_preferred_name BOOLEAN DEFAULT FALSE;
        RAISE NOTICE 'Coluna use_preferred_name adicionada';
    END IF;
END $$;

-- Verificação
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'core' AND table_name = 'evo_members'
  AND column_name IN ('code_totalpass', 'user_id_gurupass', 'client_with_promotional_restriction', 
                      'register_name', 'register_last_name', 'use_preferred_name')
ORDER BY column_name;

SELECT '✅ Colunas adicionadas com sucesso!' AS status;
