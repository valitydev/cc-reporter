## Как живёт отчёт

- Сразу после `createReport` отчёт попадает в `pending`. Это просто запись о том, что отчёт заказан и ждёт воркера.

- Когда воркер забирает его в работу, статус меняется на `processing`. В этот момент отчёт реально строится.

- Если всё прошло нормально, отчёт переходит в `created`. Это значит, что файл уже собран, загружен в storage и его
  можно скачать.

- Если во время построения что-то сломалось, отчёт либо вернётся в ожидание новой попытки, либо закончится в `failed`,
  если попытки закончились.

- Если воркер завис или пропал и отчёт слишком долго висит в работе, его переводят в `timed_out`.

- Если пользователь успел отменить отчёт, пока он ещё не дошёл до готового файла, он становится `canceled`.

- У готового отчёта есть срок жизни. Когда он заканчивается, статус меняется с `created` на `expired`. Это нормальный
  финал для успешного отчёта: он был доступен какое-то время, потом протух.

## Вычитывание полей `amount`, `provider`, `original` из потока событий

### Платежи

Класс: `PaymentEventProjector`

- `InvoicePaymentStarted`
    - при первом старте платежа заполняются основные денежные поля;
    - `amount` и `currency` берутся из `payment.cost`;
    - `providerId` и `terminalId` берутся из `started.route`, если маршрут уже есть;
    - `originalAmount` и `originalCurrency` на этом шаге тоже ставятся из `payment.cost`.

- `InvoicePaymentRouteChanged`
    - обновляет только маршрут;
    - `providerId` и `terminalId` перечитываются из нового `route`.

- `InvoicePaymentCashChanged`
    - обновляет сумму платежа;
    - `amount` и `currency` берутся из `newCash`.

- `InvoicePaymentCashFlowChanged`
    - пересчитывает денежные значения по проводкам;
    - `amount` берётся через `DomainCashFlowExtractor.extractPaymentAmount(...)`;
    - дополнительно здесь же обновляется `fee`;
    - `originalAmount` и `originalCurrency` это событие не меняет.

- `InvoicePaymentStatusChanged`
    - не меняет пользовательскую сумму платежа;
    - обновляет провайдерскую сторону расчёта:
    - `providerAmount` и `providerCurrency` берутся из `capturedCost`, если он есть в статусе.

Итого по платежу:

- обычная сумма платежа сначала приходит из `InvoicePaymentStarted`, потом может поменяться через
  `InvoicePaymentCashChanged` или `InvoicePaymentCashFlowChanged`;
- провайдер определяется сначала в `InvoicePaymentStarted`, потом может быть переопределён через
  `InvoicePaymentRouteChanged`;
- `originalAmount` и `originalCurrency` выставляются только на старте платежа и дальше этим проектором не обновляются.

### Выводы

Класс: `WithdrawalEventProjector`

- `Created`
    - на создании вывода заполняются и текущая сумма, и исходная сумма, и маршрут;
    - `amount` и `currency` берутся из `withdrawal.body`;
    - `providerId` и `terminalId` берутся из `withdrawal.route`, если маршрут уже известен;
    - `originalAmount` и `originalCurrency` берутся из `quote.cashFrom`, если есть котировка;
    - дополнительно провайдерская сумма заполняется из `quote.cashTo`;
    - курс `exchangeRateInternal` считается как отношение `cashTo / cashFrom`.

- `Route`
    - обновляет только маршрут;
    - `providerId` и `terminalId` перечитываются из нового `route`.

- `StatusChanged`
    - сумму, маршрут и исходную сумму не меняет.

- `Transfer -> payload.created.transfer.cashflow`
    - обновляет только `fee`;
    - `amount`, `providerId`, `originalAmount` и связанные валюты не трогает.

Итого по выводу:

- `amount` приходит из события создания и дальше этим проектором не меняется;
- провайдер приходит из события создания и может обновиться отдельным событием смены маршрута;
- `originalAmount` и `originalCurrency` приходят из котировки в событии создания и дальше не меняются.

### Сессия вывода

Класс: `WithdrawalSessionEventProjector`

Этот проектор не работает с `amount`, `provider` и `original`. Он сохраняет связь с выводом и данные по транзакции (
`trxId`, `trxSearch`).
