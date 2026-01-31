# Multi-Exchange Orderbook Ingestion — TODO Assignment

## 0) 목표
학습자는 6개 거래소의 **WebSocket 기반 Orderbook(Depth)** 데이터를 수신하여,
- (1) 메시지/필드 차이를 **정규화(normalization)** 하고
- (2) **sequence/version 기반 일관성 검증(gap/out-of-order 탐지)** 을 수행하며
- (3) 문제가 발생하면 **REST snapshot으로 복구(recovery)** 하고
- (4) 결과를 **QuestDB(시계열 DB)** 에 저장하고 **ZeroMQ로 실시간 publish**

하는 end-to-end 파이프라인을 완성합니다.

---

## 1) 제공 파일과 역할(제출 대상 아님)
- `docker-compose.yml` : QuestDB + 실행 컨테이너를 띄웁니다.
- `config.json` : 거래소/심볼 목록과 QuestDB/ZeroMQ 설정을 정의합니다.
- `requirements.txt` : Python 의존성 목록입니다.
- `zmq_subscriber.py` : publish 결과를 구독해 실시간으로 확인하는 예시입니다.
- `check_data_gaps.py` : QuestDB에 쌓인 데이터의 시간 구간/누락을 확인하는 예시입니다.
- `README.md` : 실행 방법과 포트/검증 방법을 제공합니다.

---

## 2) 제출물
- `orderbook.py` (단일 파일)
  - 제공된 `orderbook_TODO.py`를 `orderbook.py`로 이름 변경한 뒤,
  - TODO를 모두 채우고,
  - docker compose 환경에서 실제로 동작하는 것을 확인 후 제출합니다.

---

## 3) 실행 환경 세팅 예시 (Windows + VSCode + WSL2 + Docker Desktop)
> 아래는 “예시”이며, 본인 환경에 맞게 조정해도 됩니다.

1. **WSL2 설치** (Ubuntu 권장)  
2. **VSCode 설치** + `Remote - WSL` 확장 설치  
3. **Docker Desktop 설치** 후 WSL Integration 활성화  
4. PowerShell/WSL에서 아래가 동작해야 합니다.
   - `docker version`
   - `docker compose version`
5. 프로젝트 폴더에서:
   - `docker compose up -d`  
6. QuestDB UI 확인:
   - 브라우저에서 `http://localhost:9000`

---

## 4) 과제 로드맵 (권장 진행 순서)
### Phase A — 파이프라인을 “끝까지” 돌린다
1. QuestDB 컨테이너가 올라오는지 확인 (UI 접속)
2. Python 컨테이너가 실행되며 로그가 나오는지 확인
3. `zmq_subscriber.py`로 메시지가 실제로 흘러오는지 확인

### Phase B — 6개 거래소 구현을 완성한다 (핵심 TODO)
1. Bitget / Huobi / Coinbase **WS 구독(subscribe) 구현**
2. 각 거래소 메시지를 `OrderBookUpdate`로 변환(parse)
3. gap/out-of-order 발생 시 `fetch_snapshot()`으로 복구 로직이 동작하도록 구현
4. QuestDB에 row가 계속 쌓이는지 확인

### Phase C — 안정화/품질 개선 (권장)
1. 단위/타임스탬프 일관성 확인 (microseconds)
2. sequence 검증 로직을 거래소 문서에 맞게 더 정교화 (보너스)
3. 재연결(backoff), rate-limit, 오류 로깅 개선

---

## 5) 참고 문서(반드시 읽고 구현)
- Binance depth stream: https://docs.binance.com/
- Bybit orderbook (v5): https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
- OKX books channel (v5): https://www.okx.com/docs-v5/
- Bitget depth WS: https://bitgetlimited.github.io/apidoc/en/spot/
- Huobi depth/mbp WS: https://huobiapi.github.io/docs/spot/v1/en/
- Coinbase Advanced Trade WS level2: https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels#level2-channel

---

## 6) 채점 기준(예시)
- 필수(통과 기준)
  - 6개 거래소 연결 + 구독 성공
  - 메시지 파싱 → QuestDB 저장
  - ZMQ publish 확인 가능
- 가산점(보너스)
  - 거래소별 정확한 sequence 규칙 반영(예: Binance U/u)
  - checksum 기반 검증(Bitget/OKX 등)
  - 재연결 및 복구 안정성 향상(장시간 실행)

---

## 7) 트러블슈팅 체크리스트
- `docker` 명령어가 인식되지 않음 → Docker Desktop 설치/WSL integration/환경변수 확인
- QuestDB에 데이터가 안 쌓임 → table 이름/host 설정/ILP 포트(9009) 확인
- 특정 거래소만 안 됨 → 해당 거래소 WS 문서, 심볼 포맷, subscribe payload 확인
