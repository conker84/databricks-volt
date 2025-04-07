grammar SQLParserBase;

@members {
  /**
   * Verify whether current token is a valid decimal token (which contains dot).
   * Returns true if the character that follows the token is not a digit or letter or underscore.
   *
   * For example:
   * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
   * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
   * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
   * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is folllowed
   * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
   * which is not a digit or letter or underscore.
   */
  public boolean isValidDecimal() {
    int nextChar = _input.LA(1);
    if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
      nextChar == '_') {
      return false;
    } else {
      return true;
    }
  }
}

tokens {
    DELIMITER
}

singleStatement
    : statement ';'* EOF
    ;

// If you add keywords here that should not be reserved, add them to 'nonReserved' list.
statement
    : SHOW TABLES EXTENDED
        (WHERE filters=predicateToken)?                                         #showTablesExtended
    | SHOW TABLE CONSTRAINTS source=qualifiedName                               #showTableConstraints
    | cloneCatalogHeader (FULL)? (DEEP|SHALLOW) CLONE source=qualifiedName
       (MANAGED LOCATION location=stringLit)?                                   #cloneCatalog
    | cloneSchemaHeader (FULL)? (DEEP|SHALLOW) CLONE source=qualifiedName
       (MANAGED LOCATION location=stringLit)?                                   #cloneSchema
    | cloneTableHeader FULL (DEEP|SHALLOW) CLONE source=qualifiedName
       (TBLPROPERTIES tableProps=propertyList)?
       (LOCATION location=stringLit)?                                           #cloneTableFull
    | .*?                                                                       #passThrough
    ;

createCatalogHeader
    : CREATE CATALOG (IF NOT EXISTS)? catalog=qualifiedName
    ;

cloneCatalogHeader
    : createCatalogHeader
    ;

createSchemaHeader
    : CREATE SCHEMA (IF NOT EXISTS)? schema=qualifiedName
    ;

cloneSchemaHeader
    : createSchemaHeader
    ;

createTableHeader
    : CREATE TABLE (IF NOT EXISTS)? schema=qualifiedName
    ;

replaceTableHeader
    : (CREATE OR)? REPLACE TABLE schema=qualifiedName
    ;

cloneTableHeader
    : createSchemaHeader
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

identifier
    : IDENTIFIER             #unquotedIdentifier
    | quotedIdentifier       #quotedIdentifierAlternative
    | nonReserved            #unquotedIdentifier
    ;

propertyList
    : LEFT_PAREN property (COMMA property)* RIGHT_PAREN
    ;

property
    : key=propertyKey (EQ? value=propertyValue)?
    ;

propertyKey
    : identifier (DOT identifier)*
    | stringLit
    ;

propertyValue
    : INTEGER_VALUE
    | DECIMAL_VALUE
    | booleanValue
    | identifier LEFT_PAREN stringLit COMMA stringLit RIGHT_PAREN
    | value=stringLit
    ;

booleanValue
    : TRUE | FALSE
    ;

stringLit
    : STRING
    | DOUBLEQUOTED_STRING
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
FULL: 'FULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
TBLPROPERTIES: 'TBLPROPERTIES';
LEFT_PAREN: '(';
RIGHT_PAREN: ')';
COMMA: ',';
DOT: '.';
CONSTRAINTS: 'CONSTRAINTS';
FOR: 'FOR';

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

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT? {isValidDecimal()}?
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
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