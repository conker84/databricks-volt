grammar SQLParserBase;

tokens {
    DELIMITER
}

singleStatement
    : statement ';'* EOF
    ;

// If you add keywords here that should not be reserved, add them to 'nonReserved' list.
statement
    : SHOW TABLES EXTENDED
        (WHERE filters=predicateToken)?                                          #showTablesExtended
    | cloneCatalogHeader (DEEP|SHALLOW) CLONE source=qualifiedName
       (MANAGED LOCATION location=stringLit)?                                   #cloneCatalog
    | cloneSchemaHeader (DEEP|SHALLOW) CLONE source=qualifiedName
       (MANAGED LOCATION location=stringLit)?                                   #cloneSchema
    | .*?                                                                       #passThrough
    ;

createCatalogHeader
    : CREATE CATALOG (IF NOT EXISTS)? catalog=qualifiedName
    ;

replaceCatalogHeader
    : (CREATE OR)? REPLACE CATALOG catalog=qualifiedName
    ;

cloneCatalogHeader
    : createCatalogHeader
    | replaceCatalogHeader
    ;

createSchemaHeader
    : CREATE SCHEMA (IF NOT EXISTS)? schema=qualifiedName
    ;

replaceSchemaHeader
    : (CREATE OR)? REPLACE SCHEMA schema=qualifiedName
    ;

cloneSchemaHeader
    : createSchemaHeader
    | replaceSchemaHeader
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

stringLit
    : STRING
    | DOUBLEQUOTED_STRING
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

// We don't have an expression rule in our grammar here, so we just grab the tokens and defer
// parsing them to later. Although this is the same as `exprToken`, we have to re-define it to
// workaround an ANTLR issue (https://github.com/delta-io/delta/issues/1205)
predicateToken
    :  .+?
    ;

// Add keywords here so that people's queries don't break if they have a column name as one of
// these tokens
nonReserved
    : TABLE | TABLES | SCHEMA
    | CLONE | SHALLOW | DEEP
    | CATALOG | EXTENDED | EXISTS
    ;

// Define how the keywords above should appear in a user's SQL statement.
SHOW: 'SHOW';
CREATE: 'CREATE';
CATALOG: 'CATALOG';
IF: 'IF';
MANAGED: 'MANAGED';
LOCATION: 'LOCATION';
NOT: 'NOT' | '!';
OR: 'OR';
TABLE: 'TABLE';
TABLES: 'TABLES';
WHERE: 'WHERE';
EXTENDED: 'EXTENDED';
DEEP: 'DEEP';
SHALLOW: 'SHALLOW';
CLONE: 'CLONE';
EXISTS: 'EXISTS';
REPLACE: 'REPLACE';
SCHEMA: 'SCHEMA';
IN: 'IN';

// Multi-character operator tokens need to be defined even though we don't explicitly reference
// them so that they can be recognized as single tokens when parsing. If we split them up and
// end up with expression text like 'a ! = b', Spark won't be able to parse '! =' back into the
// != operator.
EQ  : '=' | '==';
NSEQ: '<=>';
NEQ : '<>';
NEQJ: '!=';
LTE : '<=' | '!>';
GTE : '>=' | '!<';
CONCAT_PIPE: '||';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

DOUBLEQUOTED_STRING
    :'"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

WS  : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;