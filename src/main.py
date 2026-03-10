import argparse
import json

from config import (
    DEFAULT_MODEL,
    DEFAULT_TEMPERATURE,
    DEFAULT_STRICT_MODE,
    DEFAULT_CLARIFY_LLM_POLISH,
)
from core.mcp import MCPRequest, MCPService
from llm.ollama_client import OllamaClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CAPS Phase-1 MCP <-> Ollama runner")
    parser.add_argument("--prompt", required=True, help="Prompt to send through MCP")
    parser.add_argument("--user-id", default="local-user", help="User identifier")
    parser.add_argument("--model", default=DEFAULT_MODEL, help="Ollama model name")
    parser.add_argument(
        "--temperature", type=float, default=DEFAULT_TEMPERATURE, help="Sampling temperature"
    )
    parser.add_argument(
        "--structured-intent",
        action="store_true",
        help="Return validated StructuredIntent JSON instead of free-form output",
    )
    parser.add_argument(
        "--action-parse",
        action="store_true",
        help="Return action-first parse JSON",
    )
    parser.add_argument(
        "--strict",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_STRICT_MODE,
        help="Enable strict clarification/verification gates (default from CAPS_STRICT_MODE env)",
    )
    parser.add_argument(
        "--clarify-llm-polish",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_CLARIFY_LLM_POLISH,
        help="Use LLM to polish clarification question wording (deterministic gating still authoritative)",
    )

    parser.add_argument(
    "--execute-live",
    action="store_true",
    help="Execute compiled plan using adapter runtime after planning",
    
    )
    parser.add_argument(
        "--use-langgraph",
        action="store_true",
        help="Run action-parse pipeline through LangGraph wrapper",
    )

    parser.add_argument(
        "--thread-id",
        default=None,
        help="LangGraph thread/checkpoint identifier",
    )
    parser.add_argument(
        "--sqlite-path",
        default=".caps_state.sqlite",
        help="SQLite checkpoint path for LangGraph runs",
    )
    parser.add_argument(
        "--manifest-path",
        default="src/manifest.json",
        help="Manifest JSON path for LangGraph policy/tool context",
    )
    parser.add_argument(
        "--show-langgraph-state",
        action="store_true",
        help="Show persisted LangGraph state for the given thread ID",
    )
    parser.add_argument(
        "--show-langgraph-history",
        action="store_true",
        help="Show persisted LangGraph state history for the given thread ID",
    )
    parser.add_argument(
        "--history-limit",
        type=int,
        default=10,
        help="Maximum number of LangGraph history snapshots to return",
    )

    parser.add_argument(
        "--resume-review",
        choices=["approve", "reject"],
        default=None,
        help="Resume a paused human review with an approval decision",
    )






    return parser


def main() -> None:
    args = build_parser().parse_args()
    llm = OllamaClient(model=args.model)
    mcp = MCPService(
        llm_client=llm,
        strict_mode=args.strict,
        clarify_llm_polish=args.clarify_llm_polish,
    )
    request = MCPRequest(user_id=args.user_id, prompt=args.prompt, temperature=args.temperature)

    effective_thread_id = args.thread_id or f"{args.user_id}:default"


    if args.use_langgraph and args.show_langgraph_state:
        result = mcp.get_action_parse_langgraph_state(
            thread_id=effective_thread_id,
            sqlite_path=args.sqlite_path,
        )
    elif args.use_langgraph and args.show_langgraph_history:
        result = mcp.get_action_parse_langgraph_history(
            thread_id=effective_thread_id,
            sqlite_path=args.sqlite_path,
            limit=args.history_limit,
        )
    elif args.use_langgraph and args.resume_review:
        result = mcp.resume_action_parse_langgraph(
            thread_id=effective_thread_id,
            sqlite_path=args.sqlite_path,
            decision=args.resume_review,
        )
    elif args.action_parse:
        if args.use_langgraph:
            result = mcp.process_action_parse_langgraph(
                request,
                execute_live=args.execute_live,
                thread_id=args.thread_id,
                sqlite_path=args.sqlite_path,
                manifest_path=args.manifest_path,
            )
        else:
            result = mcp.process_action_parse(request, execute_live=args.execute_live)
    elif args.structured_intent:
        result = mcp.process_structured_intent(request)
    else:
        if args.use_langgraph:
            result = mcp.process_action_parse_langgraph(
                request,
                execute_live=args.execute_live,
                thread_id=args.thread_id,
                sqlite_path=args.sqlite_path,
                manifest_path=args.manifest_path,
            )
        else:
            result = mcp.process_action_parse(request, execute_live=args.execute_live)

    print(json.dumps(result, indent=2))
   
    


if __name__ == "__main__":
    main()
