### Проектирование хранилища данные под сервис доставки

#### 1. Список полей, которые необходимы для витрины.

    courier_id — ID курьера, которому перечисляем.

    courier_name — Ф. И. О. курьера.

    settlement_year — год отчёта.

    settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.

    orders_count — количество заказов за период (месяц).

    orders_total_sum — общая стоимость заказов.

    rate_avg — средний рейтинг курьера по оценкам пользователей.

    order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.

    courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).

    courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.

    courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).

#### 2. Список таблиц в слое DDS, из которых будем брать поля для витрины.

    Есть в хранилище:
    ---------------
    dds.dm_timestamps - справочник дат заказов
    dds.dm_orders - справочник заказов

    Необходимо добавить в хранилище:
    ---------------
    dds.dm_couriers — справочник курьерьеров
	dds.dm_deliveries — справочник доставок
	dds.fct_deliveries — факты доставок

#### 3. Список сущностей и полей, которые необходимо загрузить из API.

	/couriers - данные о курьерах
        _id — ID курьера в БД;
        name — имя курьера.

	/deliveries - данные о доставках
        order_id — ID заказа;
        delivery_id — ID доставки;
        courier_id — ID курьера;
        address - адрес доставки;
        delivery_ts — дата и время совершения доставки;
        rate — рейтинг доставки, который выставляет покупатель: целочисленное значение от 1 до 5;
        tip_sum — сумма чаевых, которые оставил покупатель курьеру (в руб.).