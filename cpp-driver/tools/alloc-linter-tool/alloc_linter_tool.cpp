/*
  Copyright (c) DataStax, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#include "clang/ASTMatchers/ASTMatchFinder.h"
#include "clang/ASTMatchers/ASTMatchers.h"
#include "clang/Frontend/FrontendActions.h"
#include "clang/Tooling/CommonOptionsParser.h"
#include "clang/Tooling/Tooling.h"
#include "llvm/Support/CommandLine.h"

using namespace clang::tooling;
using namespace llvm;

using namespace clang;
using namespace clang::ast_matchers;

namespace {
StatementMatcher CXXNewMatcher = cxxNewExpr().bind("useNew");
StatementMatcher CXXDeleteMatcher = cxxDeleteExpr().bind("useDelete");
} // namespace

class AllocLinter : public MatchFinder::MatchCallback {
public:
  virtual void run(const MatchFinder::MatchResult& Result) {
    auto& Context = *Result.Context;
    auto& DiagnosticsEngine = Context.getDiagnostics();
    auto& SourceManager = Context.getSourceManager();

    const auto* NewExpr = Result.Nodes.getNodeAs<CXXNewExpr>("useNew");
    if (NewExpr &&
        !SourceManager.isInSystemHeader(NewExpr->getLocStart()) && // Not in system headers
        NewExpr->getNumPlacementArgs() == 0) {                     // Not placement new
      FunctionDecl* NewFuncDecl = NewExpr->getOperatorNew();
      if (NewExpr->isGlobalNew()) {
        diag(NewExpr->getLocStart(), "Using global `::operator new%0()`", DiagnosticsEngine)
            << (NewExpr->isArray() ? "[]" : "");
      } else if (NewFuncDecl && SourceManager.isInSystemHeader(NewFuncDecl->getLocation())) {
        diag(NewExpr->getLocStart(), "Using `operator new%0()` from %1", DiagnosticsEngine)
            << (NewExpr->isArray() ? "[]" : "")
            << NewFuncDecl->getLocation().printToString(SourceManager);
      }
    }

    const auto* DeleteExpr = Result.Nodes.getNodeAs<CXXDeleteExpr>("useDelete");
    if (DeleteExpr && !DeleteExpr->getDestroyedType().isNull() &&
        !DeleteExpr->getDestroyedType()->isVoidType() &&
        !SourceManager.isInSystemHeader(DeleteExpr->getLocStart())) { // Not in system headers
      FunctionDecl* DeleteFuncDecl = DeleteExpr->getOperatorDelete();
      if (DeleteExpr->isGlobalDelete()) {
        diag(DeleteExpr->getLocStart(), "Using global `::operator delete%0()`", DiagnosticsEngine)
            << (DeleteExpr->isArrayForm() ? "[]" : "");
      } else if (DeleteFuncDecl && SourceManager.isInSystemHeader(DeleteFuncDecl->getLocation())) {
        diag(DeleteExpr->getLocStart(), "Using `operator delete%0()` from %1", DiagnosticsEngine)
            << (DeleteExpr->isArrayForm() ? "[]" : "")
            << DeleteFuncDecl->getLocation().printToString(SourceManager);
      }
    }
  }

  DiagnosticBuilder diag(SourceLocation Loc, StringRef Description,
                         DiagnosticsEngine& DiagnosticsEngine) {
    auto ID = DiagnosticsEngine.getDiagnosticIDs()->getCustomDiagID(clang::DiagnosticIDs::Error,
                                                                    Description);
    return DiagnosticsEngine.Report(Loc, ID);
  }
};

static llvm::cl::OptionCategory AllocLinterToolCategory("alloc-linter options");

static cl::extrahelp CommonHelp(CommonOptionsParser::HelpMessage);

static cl::extrahelp MoreHelp("\nThis is a tool for finding global "
                              "use of operator new/delete (from either global\n"
                              "or from the standard library) and other forms "
                              "of global allocation.\n");

int main(int argc, const char** argv) {
  CommonOptionsParser OptionsParser(argc, argv, AllocLinterToolCategory);
  ClangTool Tool(OptionsParser.getCompilations(), OptionsParser.getSourcePathList());

  AllocLinter Linter;
  MatchFinder Finder;
  Finder.addMatcher(CXXNewMatcher, &Linter);
  Finder.addMatcher(CXXDeleteMatcher, &Linter);

  return Tool.run(newFrontendActionFactory(&Finder).get());
}
