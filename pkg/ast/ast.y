%{
package ast

import (
    "fmt"
    "strings"

    "go/constant"
    "github.com/elliotcourant/arkdb/pkg/tree"
)

// MaxUint is the maximum value of an uint.
const MaxUint = ^uint(0)
// MaxInt is the maximum value of an int.
const MaxInt = int(MaxUint >> 1)

%}

%token <src> ALTER

%token <src> BEGIN

%token <src> COMMIT
%token <src> CREATE

%token <src> DATABASE

%token <str> IDENT


%union {
  id    int32
  pos   int32
  str   string
  union sqlSymUnion
}

%type <tree.Statement> stmt_block
%type <tree.Statement> stmt

%type <tree.Statement> create_ddl_stmt
%type <tree.Statement> create_database_stmt

%type <str> database_name


%%



// General name --- names that can be column, table, etc names.
name:
  IDENT

create_database_stmt:
  CREATE DATABASE database_name
  {
    $$.val = &tree.CreateDatabase{
      Name: tree.Name($3)
    }
  }

%%