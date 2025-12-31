import os
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from app.databricks_client import DatabricksClient
from llm.llm import answer_with_llm
from app.config import settings
from app.rag import filter_chunks_by_route, _first_prediction
from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI(title="mediBot API", version="1.0.0")
dbc = DatabricksClient()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# request body model
class AskRequest(BaseModel):
    question: str = Field(..., min_length=1)
    threshold: float = 0.80
    top_k: int = 5
    pool_k: int = 50
    max_dist: float = 0.85

# response body model
class AskResponse(BaseModel):
    question: str
    route: str
    confidence: float
    answer: str
    chunks_used: int
    chunks: List[Dict[str, Any]]

def _extract_prediction(resp: Dict[str, Any]) -> Dict[str, Any]:
    preds = resp.get("predictions") or []
    
    if not preds:
        return {}
    
    return preds[0]

def _filter_chunks_by_route(chunks: List[Dict[str, Any]], route: str) -> List[Dict[str, Any]]:
    if route in (None, "", "all"):
        return chunks
    return [c for c in chunks if (c.get("category") or "").lower() == route.lower()]

@app.get("/ui", response_class=HTMLResponse)
def ui(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    q = (req.question or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    # classify
    try:
        cls_resp = dbc.classify(q, req.threshold)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Classifier call failed: {e}")

    cls_pred = _extract_prediction(cls_resp)
    route = cls_pred.get("route", "all")
    confidence = float(cls_pred.get("confidence", 0.0))

    # retrieve
    try:
        ret_resp = dbc.retrieve(q, req.top_k, req.pool_k, req.max_dist)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Retriever call failed: {e}")

    ret_pred = _extract_prediction(ret_resp)
    chunks = ret_pred.get("chunks") or []

    # apply route filter
    routed_chunks = _filter_chunks_by_route(chunks, route)

    # fallback if route filtering removes everything
    if not routed_chunks:
        routed_chunks = chunks

    # LLM answer using chunk_text only
    answer = answer_with_llm(q, routed_chunks)

    return AskResponse(
        question=q,
        route=route,
        confidence=confidence,
        answer=answer,
        chunks_used=len(routed_chunks),
        chunks=routed_chunks,
    )

@app.post("/chat")
def chat(msg: str = Form(...)):
    q = (msg or "").strip()
    if not q:
        return JSONResponse({"answer": "Please type a question."})

    # Reuse your existing /ask logic directly
    try:
        # Call your internal pipeline the same way your /ask does
        cls_resp = dbc.classify(q, settings.CLASSIFIER_THRESHOLD)
        cls_pred = _first_prediction(cls_resp)
        route = cls_pred.get("route", "all")
        confidence = float(cls_pred.get("confidence", 0.0))

        ret_resp = dbc.retrieve(q, settings.TOP_K, settings.POOL_K, settings.MAX_DIST)
        ret_pred = _first_prediction(ret_resp)
        chunks = ret_pred.get("chunks") or []

        routed_chunks = _filter_chunks_by_route(chunks, route)
        if not routed_chunks:
            routed_chunks = chunks

        answer = answer_with_llm(q, routed_chunks)

        return JSONResponse({"answer": answer})
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Chat failed: {e}")