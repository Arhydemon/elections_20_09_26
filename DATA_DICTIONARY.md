# Словарь данных

## `elections.jsonl`

Основные поля: `id`, `vrn`, `externalId`, `name`, `startVotingDay`, `finishVotingDay`, `status`, `electionLevel`, `kind`, `systemType`, `subjectRf`, `isDegPermitted`.

## `associations.jsonl`

- `extId` — идентификатор объединения;
- `namOo` — название;
- `numRegOo`, `dateRegister`, `datRegOo` — регистрационные сведения;
- `subjCod` — код субъекта;
- `urDeist` — опубликованный признак юридического статуса.

## `candidates.jsonl`

- `id` — идентификатор регистрационной записи;
- `fullName`, `birthDate` — ФИО и дата рождения;
- `mode` — вариант/режим списка источника;
- `associationId`, `electionAssociation` — объединение;
- `districtNum` — номер одномандатного округа;
- `nomination`, `enrollment` — сведения о выдвижении и включении;
- `regDate` — дата регистрации;
- `regionalGroupNum`, `numberInList` — региональная группа и номер в списке.

## `candidate_cards.jsonl`

- `candidateVrn`, `id` — идентификаторы кандидата/карточки;
- `fullName`, `surname`, `name`, `patronymic` — ФИО;
- `birthDate`, `birthPlace` — дата и место рождения;
- `regAddressPublic` — опубликованная часть адреса;
- `education`, `work`, `position` — образование, место работы и должность;
- `parlament`, `convictions`, `foreignAgent`, `status` — опубликованные дополнительные сведения и статус.

## `commissions.jsonl`

- `id`, `externalId`, `classifierMatch` — идентификаторы комиссии;
- `name`, `type`, `number` — название, тип и номер;
- `hasChildren`, `children` — наличие и список нижестоящих комиссий;
- `protocol1Signed`, `protocol2Signed` — признаки подписания форм протокола;
- `_subjectRfCode` — код субъекта РФ;
- `_isDegPermittedInherited` — унаследованный признак ДЭГ.

## `result_rows.part-*.jsonl.gz`

- `commissionClassifierId` — комиссия, к которой относится строка;
- `parentCommissionClassifierId` — вышестоящая комиссия;
- `protocolId`, `protocolNum` — идентификатор и номер формы протокола;
- `signed`, `signDateTime` — сведения о подписании;
- `isDeg` — признак дистанционного электронного голосования;
- `id` — идентификатор строки;
- `isElected` — опубликованный признак избрания;
- `category`, `descrCategory`, `group` — группировка строки в форме;
- `infoNum`, `infoPrintNum` — номер строки и печатный номер;
- `infoText` — название показателя, партии или ФИО кандидата;
- `value` — числовое значение;
- `percentValue` — процент, если опубликован.

## `report_responses.jsonl`

- `sourceGroup`, `reportType` — группа и тип отчёта;
- `request` — параметры исходного запроса;
- `id`, `createdAt` — идентификатор и время ответа;
- `body` — содержимое ответа. Структура `body` зависит от вида отчёта.

