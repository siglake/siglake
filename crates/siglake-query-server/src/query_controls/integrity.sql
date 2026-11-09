-- Canonical, OID-independent PostgreSQL catalog projection. Compared with the
-- reviewed DB-layout-v2 manifest on EVERY checkout, never learned at runtime.
-- Policy/wire schema_version remains 1; this projection includes exact new
-- audit columns, constraints, indexes, triggers, functions and every ACL.
SELECT jsonb_build_object(
 'schema', (SELECT jsonb_build_object('owner',pg_get_userbyid(nspowner),'acl',
   (SELECT jsonb_agg(jsonb_build_array(COALESCE(r.rolname,'PUBLIC'),a.privilege_type,a.is_grantable) ORDER BY COALESCE(r.rolname,'PUBLIC'),a.privilege_type)
    FROM aclexplode(COALESCE(nspacl,acldefault('n',nspowner))) a LEFT JOIN pg_roles r ON r.oid=a.grantee)) FROM pg_namespace WHERE nspname='query_policy'),
 'tables', (SELECT jsonb_agg(jsonb_build_object(
   'name',c.relname,'owner',pg_get_userbyid(c.relowner),'kind',c.relkind,'persistence',c.relpersistence,
   'rls',c.relrowsecurity,'force',c.relforcerowsecurity,'options',c.reloptions,
   'acl',(SELECT jsonb_agg(jsonb_build_array(COALESCE(r.rolname,'PUBLIC'),a.privilege_type,a.is_grantable) ORDER BY COALESCE(r.rolname,'PUBLIC'),a.privilege_type) FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a LEFT JOIN pg_roles r ON r.oid=a.grantee),
   'columns',(SELECT jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),(SELECT jsonb_build_array(n.nspname,co.collname) FROM pg_collation co JOIN pg_namespace n ON n.oid=co.collnamespace WHERE co.oid=a.attcollation),a.attnotnull,a.attidentity,a.attgenerated,a.attisdropped,(SELECT jsonb_agg(jsonb_build_array(COALESCE(r.rolname,'PUBLIC'),x.privilege_type,x.is_grantable) ORDER BY COALESCE(r.rolname,'PUBLIC'),x.privilege_type) FROM aclexplode(CASE WHEN cardinality(a.attacl)>0 THEN a.attacl ELSE NULL END) x LEFT JOIN pg_roles r ON r.oid=x.grantee),pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=c.oid AND a.attnum>0),
   'constraints',(SELECT jsonb_agg(jsonb_build_array(conname,contype,convalidated,condeferrable,condeferred,connoinherit,pg_get_constraintdef(oid)) ORDER BY conname) FROM pg_constraint WHERE conrelid=c.oid),
   'policies',(SELECT jsonb_agg(jsonb_build_array(p.polname,p.polcmd,p.polpermissive,(SELECT jsonb_agg(COALESCE(r.rolname,'PUBLIC') ORDER BY COALESCE(r.rolname,'PUBLIC')) FROM unnest(p.polroles) x LEFT JOIN pg_roles r ON r.oid=x),pg_get_expr(p.polqual,p.polrelid),pg_get_expr(p.polwithcheck,p.polrelid)) ORDER BY p.polname) FROM pg_policy p WHERE p.polrelid=c.oid),
   'triggers',(SELECT jsonb_agg(jsonb_build_array(t.tgname,t.tgenabled,t.tgisinternal,pg_get_triggerdef(t.oid)) ORDER BY t.tgname) FROM pg_trigger t WHERE t.tgrelid=c.oid),
   'indexes',(SELECT jsonb_agg(jsonb_build_array(i.indisvalid,i.indisready,pg_get_indexdef(i.indexrelid)) ORDER BY pg_get_indexdef(i.indexrelid)) FROM pg_index i WHERE i.indrelid=c.oid),
   'rules',(SELECT jsonb_agg(pg_get_ruledef(r.oid) ORDER BY r.rulename) FROM pg_rewrite r WHERE r.ev_class=c.oid)
 ) ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='query_policy' AND c.relkind NOT IN ('i','I')),
 'functions',(SELECT jsonb_agg(jsonb_build_object('definition',pg_get_functiondef(p.oid),'owner',pg_get_userbyid(p.proowner),'acl',
   (SELECT jsonb_agg(jsonb_build_array(COALESCE(r.rolname,'PUBLIC'),a.privilege_type,a.is_grantable) ORDER BY COALESCE(r.rolname,'PUBLIC'),a.privilege_type)
    FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a LEFT JOIN pg_roles r ON r.oid=a.grantee)) ORDER BY p.proname,pg_get_function_identity_arguments(p.oid)) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='query_policy')
)
