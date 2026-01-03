/*  Анализ данных для агентства недвижимости

-- Определим аномальные значения (выбросы) по значению перцентилей:
WITH limits AS (
    SELECT
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS total_area_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS rooms_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony) AS balcony_limit,
        PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_h,
        PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height) AS ceiling_height_limit_l
    FROM real_estate.flats
),
-- Найдем id объявлений, которые не содержат выбросы:
filtered_id AS(
    SELECT id
    FROM real_estate.flats  
    WHERE 
        total_area < (SELECT total_area_limit FROM limits)
        AND (rooms < (SELECT rooms_limit FROM limits) OR rooms IS NULL)
        AND (balcony < (SELECT balcony_limit FROM limits) OR balcony IS NULL)
        AND ((ceiling_height < (SELECT ceiling_height_limit_h FROM limits)
            AND ceiling_height > (SELECT ceiling_height_limit_l FROM limits)) OR ceiling_height IS NULL)
    )
-- Выведем объявления без выбросов:
SELECT *
FROM real_estate.flats
WHERE id IN (SELECT * FROM filtered_id);


-- Задача 1: Время активности объявлений
-- Результат запроса должен ответить на такие вопросы:
-- 1. Какие сегменты рынка недвижимости Санкт-Петербурга и городов Ленинградской области 
--    имеют наиболее короткие или длинные сроки активности объявлений?
-- 2. Какие характеристики недвижимости, включая площадь недвижимости, среднюю стоимость квадратного метра, 
--    количество комнат и балконов и другие параметры, влияют на время активности объявлений? 
--    Как эти зависимости варьируют между регионами?
-- 3. Есть ли различия между недвижимостью Санкт-Петербурга и Ленинградской области по полученным результатам?

--- поиск аномалий 
WITH tb_anomalies AS ( SELECT 
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS percentile_total_area,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS percentile_rooms,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony)AS percentile_balcony,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height):: NUMERIC(10,2) AS percentile_ceiling_height_h,
                            PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height):: NUMERIC(10,2) AS percentile_ceiling_height_l
                       FROM real_estate.flats f),
--- фильтрация данных 
tb_filltred_id AS ( SELECT id
                    FROM real_estate.flats f
                    WHERE total_area<(SELECT percentile_total_area FROM tb_anomalies)
                          AND (rooms<(SELECT percentile_rooms FROM tb_anomalies) OR rooms IS NULL)
                          AND (balcony<(SELECT percentile_balcony FROM tb_anomalies) OR balcony IS NULL)
                          AND ((ceiling_height <(SELECT percentile_ceiling_height_h FROM tb_anomalies)
                                AND ceiling_height >(SELECT percentile_ceiling_height_l FROM tb_anomalies)) OR ceiling_height IS NULL)),
tb_filltred_tabel AS ( SELECT *,
                               CASE 
	                                WHEN city = 'Санкт-Петербург' THEN 'Санкт-Петербург'
                               	    ELSE 'Лен.обл'
                               END AS region,
                                CASE 
	                                WHEN days_exposition IS NULL THEN 'действующие' 
	                                WHEN days_exposition<=30 THEN 'до 1 месяца'
	                                WHEN days_exposition<=90 THEN 'до 3 месяцев'
	                                WHEN days_exposition<=181 THEN 'до 6 месяцев'
	                                ELSE 'свыше 6 месяцев'
                               END AS activity_periods,
                               last_price/total_area AS price_per_meter
                               FROM real_estate.flats f 
                               LEFT JOIN real_estate.city c USING(city_id)
                               LEFT JOIN real_estate.advertisement a USING(id)
                               WHERE type_id IN (SELECT type_id FROM real_estate."type" t WHERE TYPE='город') 
                                     AND id IN (SELECT id FROM tb_filltred_id)
                                     AND total_area>0)
---итоговая таблица 
SELECT region,
       activity_periods,
       COUNT(*)::NUMERIC(10,2) AS amount_advertisement, -- кол-во объявлений
       (100.0*COUNT(*)/SUM(COUNT(*))OVER(PARTITION BY region)::decimal)::NUMERIC(10,2) AS share_advertisement, -- доля объявлений в разрезе ЛО и СПб 
       (100.0*SUM(open_plan)/SUM(COUNT(*))OVER(PARTITION BY region)::decimal)::NUMERIC(10,4) AS share_open_plan, -- доля студий
       (100.0*SUM(is_apartment)/SUM(COUNT(*))OVER(PARTITION BY region)::decimal)::NUMERIC(10,4) AS share_open_planis_apartment, --доля апартаментов
       AVG(total_area):: NUMERIC(10,2) AS avg_total_area,--ср.площадь квартир
       AVG(price_per_meter):: NUMERIC(10,2) AS avg_price_per_meter,--ср.стоимость 1 кв.метра
       PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY rooms):: NUMERIC(10,2) AS mediana_rooms, --медианное значение комнат
       PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY balcony):: NUMERIC(10,2) AS mediana_balcony, --медианное знач. балконов
       PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY floor):: NUMERIC(10,2) AS mediana_floor,--медианное значение 
       PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY floors_total):: NUMERIC(10,2) AS mediana_floors_total, -- медианная этажность домов
       PERCENTILE_DISC(0.50) WITHIN GROUP (ORDER BY ceiling_height):: NUMERIC(10,2) AS mediana_ceiling_height, -- высота потолков
       (AVG(airports_nearest)/1000) :: NUMERIC(10,2) AS avg_airports_nearest, -- удаленность аэропортов
       (100.0*SUM(tb.parks_around3000)/SUM(COUNT(*)) OVER(PARTITION BY region)::decimal)::NUMERIC(10,2) AS share_parks,---доля парков рядом
       (100.0*SUM(tb.ponds_around3000)/SUM(COUNT(*))OVER(PARTITION BY region)::decimal)::NUMERIC(10,2) AS share_ponds --- доля прудов рядом
FROM tb_filltred_tabel AS tb
GROUP BY region, activity_periods
ORDER BY region DESC , activity_periods;


-- Задача 2: Сезонность объявлений
-- Результат запроса должен ответить на такие вопросы:
-- 1. В какие месяцы наблюдается наибольшая активность в публикации объявлений о продаже недвижимости? 
--    А в какие — по снятию? Это показывает динамику активности покупателей.
-- 2. Совпадают ли периоды активной публикации объявлений и периоды, 
--    когда происходит повышенная продажа недвижимости (по месяцам снятия объявлений)?
-- 3. Как сезонные колебания влияют на среднюю стоимость квадратного метра и среднюю площадь квартир? 
--    Что можно сказать о зависимости этих параметров от месяца?

WITH tb_anomalies AS ( SELECT 
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS percentile_total_area,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS percentile_rooms,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony)AS percentile_balcony,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height):: NUMERIC(10,2) AS percentile_ceiling_height_h,
                            PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height):: NUMERIC(10,2) AS percentile_ceiling_height_l
                       FROM real_estate.flats f),
--- фильтрация данных id  от аномальных значений
tb_filltred_id AS ( SELECT id
                    FROM real_estate.flats f
                    WHERE total_area<(SELECT percentile_total_area FROM tb_anomalies)
                          AND (rooms<(SELECT percentile_rooms FROM tb_anomalies) OR rooms IS NULL)
                          AND (balcony<(SELECT percentile_balcony FROM tb_anomalies) OR balcony IS NULL)
                          AND ((ceiling_height <(SELECT percentile_ceiling_height_h FROM tb_anomalies)
                                AND ceiling_height >(SELECT percentile_ceiling_height_l FROM tb_anomalies)) OR ceiling_height IS NULL)),
--- данные об опубликованных объявлениях 
tb_of_publishing AS (SELECT 
		                   EXTRACT (MONTH FROM first_day_exposition) AS mohth_of_publishing,
		                   RANK() OVER(ORDER BY COUNT(*) DESC) AS rank_mohth_of_publishing,
		                   COUNT(*) AS amount_of_publishing,
		                   (100.0* COUNT(*)::decimal/SUM(count(*)) OVER())::NUMERIC(10,2) AS share_of_publishing,
		                   AVG (last_price/total_area):: NUMERIC(10,2) AS avg_per_meter_of_publishin,
		                   AVG(total_area):: NUMERIC(10,2) AS avg_total_area_of_publishin
		             FROM real_estate.advertisement a 
		             LEFT JOIN real_estate.flats f USING(id)
		             WHERE id IN (SELECT id FROM tb_filltred_id)
		                   AND total_area>0
		                   AND type_id IN (SELECT type_id FROM real_estate."type" t WHERE TYPE='город') 
		             GROUP BY mohth_of_publishing),
--- данные о снятых объявлениях 
tb_of_withdrawal AS (SELECT                    
                            EXTRACT (MONTH FROM (first_day_exposition::date +  days_exposition*interval '1 day' )) AS mohth_of_withdrawal,
                            RANK() OVER(ORDER BY COUNT(*) DESC) AS rank_mohth_of_withdrawal,
                            COUNT(*) AS amount_of_withdrawal,
                            (100.0* COUNT(*)::decimal/SUM(count(*)) OVER())::NUMERIC(10,2) AS share_of_withdrawal,
                            AVG (last_price/total_area):: NUMERIC(10,2) AS avg_per_meter_of_withdrawal,
                            AVG(total_area):: NUMERIC(10,2) AS avg_total_area_of_withdrawal
                     FROM real_estate.advertisement a 
                     LEFT JOIN real_estate.flats f USING(id)
                     WHERE id IN (SELECT id FROM tb_filltred_id)
                        AND total_area>0 AND days_exposition IS NOT NULL
                        AND type_id IN (SELECT type_id FROM real_estate."type" t WHERE TYPE='город')
                   GROUP BY mohth_of_withdrawal)
---- сводные значения по публикациям
SELECT mohth_of_publishing,
       rank_mohth_of_publishing,
       amount_of_publishing,
       share_of_publishing,
       avg_per_meter_of_publishin,
       avg_total_area_of_publishin,
       mohth_of_withdrawal,
       rank_mohth_of_withdrawal,
       amount_of_withdrawal,
       share_of_withdrawal,
       avg_per_meter_of_withdrawal,
       avg_total_area_of_withdrawal
FROM tb_of_publishing AS tbp
LEFT JOIN tb_of_withdrawal AS tw ON tbp.mohth_of_publishing = tw.mohth_of_withdrawal
ORDER BY rank_mohth_of_publishing;

-- Задача 3: Анализ рынка недвижимости Ленобласти
-- Результат запроса должен ответить на такие вопросы:
-- 1. В каких населённые пунктах Ленинградской области наиболее активно публикуют объявления о продаже недвижимости?
-- 2. В каких населённых пунктах Ленинградской области — самая высокая доля снятых с публикации объявлений? 
--    Это может указывать на высокую долю продажи недвижимости.
-- 3. Какова средняя стоимость одного квадратного метра и средняя площадь продаваемых квартир в различных населённых пунктах? 
--    Есть ли вариация значений по этим метрикам?
-- 4. Среди выделенных населённых пунктов какие пункты выделяются по продолжительности публикации объявлений? 
--    То есть где недвижимость продаётся быстрее, а где — медленнее.

WITH tb_anomalies AS ( SELECT 
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY total_area) AS percentile_total_area,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY rooms) AS percentile_rooms,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY balcony)AS percentile_balcony,
                            PERCENTILE_DISC(0.99) WITHIN GROUP (ORDER BY ceiling_height):: NUMERIC(10,2) AS percentile_ceiling_height_h,
                            PERCENTILE_DISC(0.01) WITHIN GROUP (ORDER BY ceiling_height):: NUMERIC(10,2) AS percentile_ceiling_height_l
                       FROM real_estate.flats f),
--- фильтрация данных id  от аномальных значений
tb_filltred_id AS ( SELECT id
                    FROM real_estate.flats f
                    WHERE total_area<(SELECT percentile_total_area FROM tb_anomalies)
                          AND (rooms<(SELECT percentile_rooms FROM tb_anomalies) OR rooms IS NULL)
                          AND (balcony<(SELECT percentile_balcony FROM tb_anomalies) OR balcony IS NULL)
                          AND ((ceiling_height <(SELECT percentile_ceiling_height_h FROM tb_anomalies)
                                AND ceiling_height >(SELECT percentile_ceiling_height_l FROM tb_anomalies)) OR ceiling_height IS NULL)),
---выделяем данные по области 
tb_lenobl AS ( SELECT      id,
                           city,
                           TYPE,
                           first_day_exposition,
                           days_exposition,
                           last_price,
                           total_area,
                           ceiling_height,
                           balcony,
                           rooms,
                           floor
                     FROM real_estate.advertisement a 
                     JOIN real_estate.flats f  USING(id)
                     LEFT JOIN real_estate.city c USING(city_id)
                     LEFT JOIN real_estate."type" t ON f.type_id=t.type_id
                     WHERE f.city_id NOT IN ( SELECT city_id FROM real_estate.city WHERE city = 'Санкт-Петербург')
                           AND total_area>0
                           AND id IN (SELECT id FROM tb_filltred_id)),
--- расчитываем основные показатели по Лен.обл.
tb_city_lenobl AS ( SELECT 
                           city,
                           TYPE,
                           COUNT(*) AS amount_ads, 
                           (100.00 * SUM(CASE WHEN days_exposition IS NOT NULL THEN 1 ELSE 0 END)/ COUNT(*) )::NUMERIC(10,3)  AS share_withdrawal_ads,
                           AVG(last_price/total_area) ::NUMERIC(10,2) AS avg_price_per_meter,
                           AVG(total_area) ::NUMERIC(10,2) AS avg_total_area,
                           AVG(days_exposition)::NUMERIC(10,0) AS avg_days,
                           AVG (ceiling_height) AS avg_ceiling_height,
                           AVG(balcony) AS avg_balcony,
                           AVG(rooms) AS avg_rooms,
                           AVG(floor) AS avg_floor
                      FROM  tb_lenobl
                      GROUP BY city, type
                      HAVING COUNT(*)>=50
                      LIMIT 15)
SELECT 
       city,
       TYPE,
       amount_ads,
       avg_price_per_meter,
       avg_total_area,
       avg_days,
       share_withdrawal_ads,
       RANK() OVER(ORDER BY amount_ads DESC ) AS rank_ads,
       RANK() OVER(ORDER BY share_withdrawal_ads DESC ) AS rank_share,
       RANK() OVER(ORDER BY avg_days ASC ) AS rank_days
FROM tb_city_lenobl 
ORDER BY amount_ads DESC;

