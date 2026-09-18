package com.qbe.kafkastarter.mapper;

import com.qbe.avro.StockAlertState;
import com.qbe.kafkastarter.dto.StockAlertStateDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface StockAlertStateMapper {

    StockAlertState toAvro(StockAlertStateDto dto);

    StockAlertStateDto fromAvro(StockAlertState event);
}
