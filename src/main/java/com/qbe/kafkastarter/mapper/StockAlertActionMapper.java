package com.qbe.kafkastarter.mapper;

import com.qbe.avro.StockAlertAction;
import com.qbe.kafkastarter.dto.StockAlertActionDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface StockAlertActionMapper {

    StockAlertAction toAvro(StockAlertActionDto dto);

    StockAlertActionDto fromAvro(StockAlertAction event);
}
